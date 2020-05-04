(ns spartadata.handlers.sdmxapi
  (:require [clojure.data.xml :as xml]
            [environ.core :refer [env]]
            [spartadata.database.retrieve :refer [retrieve-data-message]]
            [spartadata.database.upload :refer [upload-data-message]]
            [spartadata.database.upload-historical :refer [upload-historical-data-message]]))

(defn structure [request]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) (:uri request))}
   :body {}})

(defn data [connection-pool {headers :headers {path-params :path query-params :query} :parameters}]
  (let [flow-ref-params (mapv #(clojure.string/split % #"\+") (clojure.string/split (:flow-ref path-params) #","))
        key-params (if (and (:key path-params) (not= "all" (:key path-params))) (mapv #(clojure.string/split % #"\+") (clojure.string/split (:key path-params) #"\." -1)) "all")
        provider-ref-params (if (:provider-ref path-params) (mapv #(clojure.string/split % #"\+") (clojure.string/split (:provider-ref path-params) #",")))
        data-message (try (retrieve-data-message headers
                                                 {:datasource connection-pool}
                                                 {:flow-ref (condp = (count flow-ref-params)
                                                              1 (-> (zipmap [:id] flow-ref-params) (assoc :agencyid "all") (assoc :version "latest"))
                                                              2 (-> (zipmap [:agencyid :id] flow-ref-params) (assoc :version "latest"))
                                                              3 (zipmap [:agencyid :id :version] flow-ref-params)) 
                                                  :provider-ref  (if provider-ref-params 
                                                                   (condp = (count provider-ref-params)
                                                                     1 (-> (zipmap [:id] provider-ref-params) (assoc :agencyid "all"))
                                                                     2 (zipmap [:agencyid :id] provider-ref-params)))
                                                  :key (if (not= "all" key-params) 
                                                         (if (->> (map (partial reduce str) key-params) (reduce str) empty?) "all" key-params)
                                                         key-params)}
                                                 query-params)
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (-> (cond 
          (= 100 (:error data-message)) {:status 404}
          (= 110 (:error data-message)) {:status 401}
          (= 130 (:error data-message)) {:status 413}
          (= 140 (:error data-message)) {:status 400}
          (= 150 (:error data-message)) {:status 403}
          (= 500 (:error data-message)) {:status 500}
          (= 501 (:error data-message)) {:status 501}
          (= 503 (:error data-message)) {:status 503}
          (= 510 (:error data-message)) {:status 413}
          (< 1000 (:error data-message)) {:status 500}
          :else {:status 200})
        (assoc :headers {"content-type" (:content-type data-message)})
        (assoc :body (:content data-message)))))

(defn data-upload [connection-pool request]
  {:status 201
   :body (try (upload-data-message {:datasource connection-pool} 
                                   (get-in request [:parameters :multipart :file :tempfile])
                                   (get-in request [:parameters :path])
                                   (get-in request [:parameters :query])) 
              (catch Exception e (do (.printStackTrace e) (throw e))))})

(defn data-upload-hist [connection-pool request]
  {:status 201
   :body (try (upload-historical-data-message {:datasource connection-pool} 
                                              (get-in request [:parameters :multipart :file :tempfile])
                                              (get-in request [:parameters :path])
                                              (get-in request [:parameters :query])) 
              (catch Exception e (do (.printStackTrace e) (throw e))))})

(defn data-rollback [connection-pool request]
  {:status 501
   :body {}})

(defn metadata [request]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) (:uri request))}
   :body {}})

(defn schema [request]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) (:uri request))}
   :body {}})

(defn other [request]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) (:uri request))}
   :body {}})

(ns spartadata.handlers.sdmx
  (:require [clojure.data.xml :as xml]
            [environ.core :refer [env]]
            [spartadata.database.retrieve :refer [retrieve-data-message]]))

(defn structure [request]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) (:uri request))}
   :body {}})

(defn data [connection-pool request]
  (let [path-params (get-in request [:parameters :path])
        flow-ref-params (mapv #(clojure.string/split % #"\+") (clojure.string/split (:flow-ref path-params) #","))
        key-params (if (:key path-params) (mapv #(clojure.string/split % #"\+") (clojure.string/split (:key path-params) #"\." -1)))
        provider-ref-params (if (:provider-ref path-params) (mapv #(clojure.string/split % #"\+") (clojure.string/split (:provider-ref path-params) #",")))
        query-params (get-in request [:parameters :query])]
    {:status 200
     :headers {"content-type" "application/vnd.sdmx.compact+xml;version=2.0"}
     :body (try (xml/emit-str 
                  (retrieve-data-message {:datasource connection-pool}
                                         {:flow-ref (condp = (count flow-ref-params)
                                                      1 (-> (zipmap [:id] flow-ref-params) (assoc :agencyid "all") (assoc :version "latest"))
                                                      2 (-> (zipmap [:agencyid :id] flow-ref-params) (assoc :version "latest"))
                                                      3 (zipmap [:agencyid :id :version] flow-ref-params)) 
                                          :provider-ref  (if provider-ref-params 
                                                           (condp = (count provider-ref-params)
                                                             1 (-> (zipmap [:id] provider-ref-params) (assoc :agencyid "all"))
                                                             2 (zipmap [:agencyid :id] provider-ref-params)))
                                          :key (if key-params 
                                                 (if (->> (map (partial reduce str) key-params) (reduce str) empty?) nil key-params)
                                                 "all")}))
                (catch Exception e (do (.printStackTrace e) (throw e))))}))

(defn data-upload [connection-pool request]
  (let [params (get-in request [:parameters :path])
        flow-ref-params (clojure.string/split (:flow-ref params) #",")]
    {:status 201
     :body {}}))

(defn data-upload-hist [connection-pool request]
  (let [params (get-in request [:parameters :path])
        flow-ref-params (clojure.string/split (:flow-ref params) #",")]
    {:status 201
     :body {}}))

(defn data-rollback [connection-pool request]
  (let [params (get-in request [:parameters :path])
        flow-ref-params (clojure.string/split (:flow-ref params) #",")]
    {:status 201
     :body {}}))

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

(ns spartadata.handler.sdmxapi
  (:require [clojure.data.xml :as xml]
            [environ.core :refer [env]]
            [spartadata.model.delete :refer [delete-dataset]]
            [spartadata.model.release :refer [add-release]]
            [spartadata.model.retrieve :refer [retrieve-data-message]]
            [spartadata.model.upload :refer [upload-data-message upload-historical-data-message]]
            [spartadata.model.rollback :refer [rollback-release]]
            [spartadata.model.enquire :refer [fetch-release]]
            [spartadata.sdmx.errors :refer [sdmx-response]]))



;; Data API


(defn data [connection-pool {headers :headers {path-params :path query-params :query} :parameters}]
  (let [flow-ref-params (mapv #(clojure.string/split % #"\+") (clojure.string/split (:flow-ref path-params) #","))
        key-params (if (and (:key path-params) (not= "all" (:key path-params))) 
                     (mapv #(clojure.string/split % #"\+") (clojure.string/split (:key path-params) #"\." -1)) "all")
        provider-ref-params (if (:provider-ref path-params) 
                              (mapv #(clojure.string/split % #"\+") (clojure.string/split (:provider-ref path-params) #",")))
        data-message (try (retrieve-data-message {:datasource connection-pool}
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
                                                 (update query-params :format (fn [fmt] (case fmt
                                                                                          "json" "application/json"
                                                                                          "sdmx-2.0" "application/vnd.sdmx.compact+xml;version=2.0"
                                                                                          (get headers "accept")))))
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (sdmx-response data-message)))

(defn data-upload [connection-pool {headers :headers {path-params :path query-params :query multipart :multipart} :parameters}]
  (let [data-message (try (upload-data-message {:datasource connection-pool} 
                                               (get-in multipart [:file :tempfile])
                                               (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref path-params) #","))
                                               (update query-params :format (fn [fmt] (case fmt
                                                                                        "json" "application/json"
                                                                                        "sdmx-2.0" "application/vnd.sdmx.compact+xml;version=2.0"
                                                                                        (get headers "accept"))))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (sdmx-response data-message)))

(defn data-upload-hist [connection-pool {headers :headers {path-params :path query-params :query multipart :multipart} :parameters}]
  (let [data-message (try (upload-historical-data-message {:datasource connection-pool} 
                                                          (get-in multipart [:file :tempfile])
                                                          (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref path-params) #","))
                                                          (update query-params :format (fn [fmt] (case fmt
                                                                                                   "json" "application/json"
                                                                                                   "sdmx-2.0" "application/vnd.sdmx.compact+xml;version=2.0"
                                                                                                   (get headers "accept"))))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (sdmx-response data-message)))

(defn data-rollback [connection-pool request]
  {:status 201
   :body (try (rollback-release {:datasource connection-pool} 
                                (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))) 
              (catch Exception e (do (.printStackTrace e) (throw e))))})

(defn data-releases [connection-pool request]
  (let [data-message (try (fetch-release {:datasource connection-pool} 
                                         (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))
                                         (get-in request [:parameters :query])) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (sdmx-response data-message)))

(defn data-release [connection-pool request]
  (let [data-message (try (add-release {:datasource connection-pool} 
                                       (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))
                                       (-> (get-in request [:parameters :query])
                                           (clojure.set/rename-keys {:releaseDescription :description}))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (sdmx-response data-message)))

(defn data-delete [connection-pool request]
  (let [data-message (try (delete-dataset {:datasource connection-pool} 
                                          (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (sdmx-response data-message)))

;; Redirects to Fusion Registry


(defn metadata [request]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) (:uri request))}
   :body {}})

(defn structure [{uri :uri context :context query-string :query-string}]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) 
                             (clojure.string/replace-first uri (re-pattern (str context "/sdmxapi")) "/sdmxapi/rest") 
                             (if query-string (str "?" query-string)))}
   :body "Redirecting..."})

(defn schema [{uri :uri context :context query-string :query-string}]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) 
                             (clojure.string/replace-first uri (re-pattern (str context "/sdmxapi")) "/sdmxapi/rest") 
                             (if query-string (str "?" query-string)))}
   :body "Redirecting..."})

(defn other [{uri :uri context :context query-string :query-string}]
  {:status 301
   :headers {"location" (str (:sdmx-registry env) 
                             (clojure.string/replace-first uri (re-pattern (str context "/sdmxapi")) "/sdmxapi/rest") 
                             (if query-string (str "?" query-string)))}
   :body "Redirecting..."})

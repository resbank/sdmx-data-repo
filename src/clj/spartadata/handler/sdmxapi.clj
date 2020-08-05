(ns spartadata.handler.sdmxapi
  (:require [spartadata.model.sdmx.delete :refer [delete-dataset]]
            [spartadata.model.sdmx.release :refer [add-release]]
            [spartadata.model.sdmx.retrieve :refer [retrieve-data-message]]
            [spartadata.model.sdmx.upload :refer [upload-data-message upload-historical-data-message]]
            [spartadata.model.sdmx.rollback :refer [rollback-release]]
            [spartadata.model.sdmx.enquire :refer [fetch-release]]
            [spartadata.utilities :refer [format-response]]))



;; Data API


(defn data [{headers :headers query :sdmx-query {path-params :path query-params :query} :parameters :as request}]
  (comment
    (-> (try (retrieve-data-message {:datasource (:conn request)}
                                    (:dataflows query)
                                    {:key (if (and (:key path-params) (not= "all" (:key path-params))) 
                                            (->> (clojure.string/split (:key path-params) #"\." -1)
                                                 (mapv #(clojure.string/split % #"\+"))) 
                                            "all")}
                                    (update query-params :format (fn [fmt] (case fmt
                                                                             "json" :application/json
                                                                             "sdmx-2.0" :application/vnd.sdmx.compact+xml_version-2.0
                                                                             (-> (get headers "accept")
                                                                                 (clojure.string/replace #";" "_")
                                                                                 (clojure.string/replace #"=" "-")
                                                                                 keyword)))))
             (catch Exception e (do (.printStackTrace e) (throw e))))
        format-response))
  {:status 200
   :body query})

(defn data-upload [{headers :headers {path-params :path query-params :query multipart :multipart} :parameters :as request}]
  (let [data-message (try (upload-data-message {:datasource (:conn request)} 
                                               (get-in multipart [:file :tempfile])
                                               (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref path-params) #","))
                                               (update query-params :format (fn [fmt] (case fmt
                                                                                        "json" "application/json"
                                                                                        "sdmx-2.0" "application/vnd.sdmx.compact+xml;version=2.0"
                                                                                        (get headers "accept"))))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (format-response data-message)))

(defn data-upload-hist [{headers :headers {path-params :path query-params :query multipart :multipart} :parameters :as request}]
  (let [data-message (try (upload-historical-data-message {:datasource (:conn request)} 
                                                          (get-in multipart [:file :tempfile])
                                                          (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref path-params) #","))
                                                          (update query-params :format (fn [fmt] (case fmt
                                                                                                   "json" "application/json"
                                                                                                   "sdmx-2.0" "application/vnd.sdmx.compact+xml;version=2.0"
                                                                                                   (get headers "accept"))))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (format-response data-message)))

(defn data-create [{headers :headers {path-params :path query-params :query multipart :multipart} :parameters :as request}]
  (let [data-message (try (if (:releaseDateTime query-params)
                            (upload-historical-data-message {:datasource (:conn request)} 
                                                            (get-in multipart [:file :tempfile])
                                                            (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref path-params) #","))
                                                            (update query-params :format (fn [fmt] (case fmt
                                                                                                     "json" "application/json"
                                                                                                     "sdmx-2.0" "application/vnd.sdmx.compact+xml;version=2.0"
                                                                                                     (get headers "accept")))))
                            (upload-data-message {:datasource (:conn request)} 
                                               (get-in multipart [:file :tempfile])
                                               (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref path-params) #","))
                                               (update query-params :format (fn [fmt] (case fmt
                                                                                        "json" "application/json"
                                                                                        "sdmx-2.0" "application/vnd.sdmx.compact+xml;version=2.0"
                                                                                        (get headers "accept")))))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (format-response data-message)))

(defn data-rollback [request]
  {:status 201
   :body (try (rollback-release {:datasource (:conn request)} 
                                (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))) 
              (catch Exception e (do (.printStackTrace e) (throw e))))})

(defn data-releases [request]
  (let [data-message (try (fetch-release {:datasource (:conn request)} 
                                         (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))
                                         (get-in request [:parameters :query])) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (format-response data-message)))

(defn data-release [request]
  (let [data-message (try (add-release {:datasource (:conn request)} 
                                       (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))
                                       (get-in request [:parameters :query])) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (format-response data-message)))

(defn data-release-hist [request]
  (let [data-message (try (add-release {:datasource (:conn request)} 
                                       (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))
                                       (get-in request [:parameters :query])) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (format-response data-message)))

(defn data-delete [request]
  (let [data-message (try (delete-dataset {:datasource (:conn request)} 
                                          (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #","))) 
                          (catch Exception e (do (.printStackTrace e) (throw e))))]
    (format-response data-message)))

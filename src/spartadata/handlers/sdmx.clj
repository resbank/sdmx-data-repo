(ns spartadata.handlers.sdmx
  (:require [clojure.data.xml :as xml]
            [spartadata.database.retrieve :refer [retrieve-data-message]]))

(defn data [connection-pool request]
  (let [params (get-in request [:parameters :path])
        flow-ref-params (clojure.string/split (:flow-ref params) #",")
        key-params (mapv #(clojure.string/split % #"\+") (clojure.string/split (:key params) #"\." -1))
        provider-ref-params (clojure.string/split (:provider-ref params) #",")]
    {:status 200
     :body (try (xml/emit-str 
                  (retrieve-data-message {:datasource connection-pool}
                                         {:flow-ref (condp = (count flow-ref-params)
                                                      1 (-> (zipmap [:id] flow-ref-params) (assoc :agencyid "all") (assoc :version "latest"))
                                                      2 (-> (zipmap [:agencyid :id] flow-ref-params) (assoc :version "latest"))
                                                      3 (zipmap [:agencyid :id :version] flow-ref-params)) 
                                          :provider-ref  (condp = (count(clojure.string/split (:provider-ref params) #","))
                                                           1 (-> (zipmap [:id] provider-ref-params) (assoc :agencyid "all"))
                                                           2 (zipmap [:agencyid :id] provider-ref-params))
                                          :key (if (->> (map (partial reduce str) key-params) (reduce str) empty?) nil key-params)}))
                (catch Exception e (do (.printStackTrace e) (throw e))))}))

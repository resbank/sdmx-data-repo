(ns spartadata.database.retrieve
  (:require [clojure.data.xml :as xml]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.database.upload :refer [validate-data]]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Retrieve data message


(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")

(defn retrieve-data-message [db {dataset :flow-ref dimensions :key} & {:keys [start-period end-period release validate?] :or {validate? false}}]
  "Usage: (retrieve-data-message db dimensions dataflow & {:keys [validate?] :or {validate? false}})

  Retrieve and return a (valid) data message from the database corresponding to the dimensions
  provided. Returns a clojure.data.xml.node.Element"
  (let [{agencyid :agencyid id :id version :version} dataset
        dataset-id (get-dataset-id db dataset)
        dataset-attrs (get-dataset-attrs db dataset-id)
        dimension-id-sets (for [pos (map inc (range (count dimensions)))
                                :let [values (nth dimensions (dec pos))]]
                            (map :dimension_id
                                 (if (empty? (first values))
                                   (get-dim-ids-by-pos db (assoc dataset-id :pos pos))
                                   (get-dim-ids-by-vals db (merge {:vals values :pos pos} dataset-id)))))
        ns1 (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
        xml-doc (xml/element ::messg/CompactData {}
                             (xml/element ::messg/Header {}
                                          (xml/element ::messg/ID {} "IREF01FISHPASTE")
                                          (xml/element ::messg/Test {} false)
                                          (xml/element ::messg/Prepared {} (str (java-time/local-date-time)))
                                          (xml/element ::messg/Sender {:id "SARB.ERD"})
                                          (xml/element ::messg/Receiver {:id "UNKNOWN"}))
                             (#(apply xml/element (xml/qname ns1 "DataSet") (zipmap (map (comp keyword :attr) dataset-attrs) (map :val dataset-attrs)) %) 
                                      (for [series-id (if dimensions 
                                                        (reduce #(clojure.set/intersection %1 %2) 
                                                                (pmap #(into #{} (->> (get-series-ids-from-dim-ids db {:dims %})
                                                                                      (map vals)
                                                                                      (reduce concat))) 
                                                                      (filter (comp not empty?) dimension-id-sets)))
                                                        (map :series_id (get-series-ids db dataset-id)))
                                            :let [series-dims (get-series-dims db {:series_id series-id})
                                                  series-attrs (get-series-attrs db {:series_id series-id})]]
                                        (#(apply xml/element (xml/qname ns1 "Series") (merge (zipmap (map (comp keyword :dim) series-dims) (map :val series-dims))
                                                                                             (zipmap (map (comp keyword :attr) series-attrs) (map :val series-attrs))) %)
                                                 (for [obs (if release  
                                                             (get-obs-by-release db {:release release :series_id series-id})
                                                             (get-obs db {:series_id series-id}))
                                                       :let [obs-attrs (get-obs-attrs db obs)]]
                                                   (xml/element (xml/qname ns1 "Obs") (merge {:TIME_PERIOD (str (:time_period obs)) 
                                                                                              :OBS_VALUE (:obs_value obs)}  
                                                                                             (zipmap (map (comp keyword :attr) obs-attrs) 
                                                                                                     (map :val obs-attrs)))))))))]
    (if validate? (validate-data (str (:sdmx-registry env) "schema/dataflow/" 
                                      (:agencyid dataset)  "/" (:id dataset) "/" (:version dataset) 
                                      "?format=sdmx-2.0") 
                                 (xml/emit-str xml-doc)))
    xml-doc))

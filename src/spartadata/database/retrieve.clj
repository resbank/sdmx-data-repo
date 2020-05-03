(ns spartadata.database.retrieve
  (:require [clojure.data.xml :as xml]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.database.upload :refer [validate-data]]
            [spartadata.sdmx-errors :refer [sdmx-error]]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Retrieve data message


(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")

(def message-counter (atom 0))

(declare compile-data)
(declare compile-datasets)
(declare compile-series)
(declare compile-observations)

(defn retrieve-data-message 
  "Usage: (retrieve-data-message db {dataflow :flow-ref dimensions :key unused :provider-ref} options)

  {start-period :startPeriod
  end-period :endPeriod
  updated-after :updatedAfter
  first-n-observations :firstNObservations
  last-n-observations :lastNObservations
  dimensions-at-observation :dimensionAtObservation
  detail-data-type :detailDataType
  include-history? :includeHistory
  release :release
  validate? :validate
  :or {dimension-at-observation \"TIME_PERIOD\"
  data-detail-type \"full\"
  include-history? false
  validate? false}}

  Retrieve and return a (valid) data message from the database corresponding to the dimensions
  provided. Returns a clojure.data.xml.node.Element"
  ([db {dataflow :flow-ref dimensions :key unused :provider-ref} {validate? :validate :or {validate? false} :as options}]
   (let [datasets (->> (for [agencyid (:agencyid dataflow)] 
                        (for [id (:id dataflow)] 
                          (for [version (:version dataflow)] 
                            [agencyid id version])))
                      (reduce concat)
                      (reduce concat))
         data-message (compile-data db datasets dimensions options)]
     (if (and validate? (= 1 (count datasets)) (not= "Error" (name (:tag data-message)))) 
       (let [[agencyid id version] (first datasets)]
         (validate-data (str (:sdmx-registry env) "schema/dataflow/" agencyid "/" id "/" version "?format=sdmx-2.0") 
                        (xml/emit-str data-message)))
       (if (and validate? (not= 1 (count datasets))) (println "Validation skipped: Multiple datastructures.")))
     data-message)))

(defn compile-data [db datasets dimensions options]
  (let [dataset-messages (compile-datasets db datasets dimensions options)]
    (if (empty? dataset-messages)
      (sdmx-error 100 (str "No Dataflow exists for query : Target: Dataflow"
                           " - Agency Id: " (clojure.string/join "+" (map first datasets))
                           " - Maintainable Id: " (clojure.string/join "+" (map second datasets))
                           " - Version: " (clojure.string/join "+" (map last datasets))))
      (apply xml/element 
             (concat [::messg/CompactData {}
                      (xml/element ::messg/Header {}
                                   (xml/element ::messg/ID {} (str "IREF" (swap! message-counter inc)))
                                   (xml/element ::messg/Test {} false)
                                   (xml/element ::messg/Prepared {} (str (java-time/local-date-time)))
                                   (xml/element ::messg/Sender {:id "SARB.ERD"})
                                   (xml/element ::messg/Receiver {:id "ANONYMOUS"}))]
                     dataset-messages)))))

(defn compile-datasets [db datasets dimensions options]
  ;; all/all/all|latest not implemented
  (for [dataset datasets
        :let [[agencyid id version] dataset
              ns1 (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
              dataset-id (get-dataset-id db {:agencyid agencyid :id id :version version})]
        :when dataset-id]
    (let [dataset-attrs (get-dataset-attrs db dataset-id)]
      (apply xml/element 
             (concat [(xml/qname ns1 "DataSet") 
                      (zipmap (map (comp keyword :attr) dataset-attrs) (map :val dataset-attrs))
                      (compile-series db ns1 dataset-id dimensions options)])))))

(defn compile-series [db ns1 dataset-id dimensions options]
  (let [dimension-id-sets (if (not= "all" dimensions) 
                            (->> (for [pos (map inc (range (count dimensions)))
                                       :let [values (nth dimensions (dec pos))]]
                                   (map :dimension_id
                                        (if (empty? (first values))
                                          (get-dim-ids-by-pos db (assoc dataset-id :pos pos))
                                          (get-dim-ids-by-vals db (merge {:vals values :pos pos} dataset-id)))))
                                 (filter (comp not empty?))))]
    (for [series-id (if (and (not= "all" dimensions)) 
                      (if ((comp not empty?) dimension-id-sets)
                        (reduce #(clojure.set/intersection %1 %2) 
                                (pmap #(into #{} (->> (get-series-ids-from-dim-ids db {:dims %})
                                                      (map vals)
                                                      (reduce concat))) 
                                      dimension-id-sets)))
                      (map :series_id (get-series-ids db dataset-id)))
          :let [series-dims (get-series-dims db {:series_id series-id})
                series-attrs (get-series-attrs db {:series_id series-id})]]
      (apply xml/element 
             (concat [(xml/qname ns1 "Series") 
                      (merge (zipmap (map (comp keyword :dim) series-dims) (map :val series-dims))
                             (zipmap (map (comp keyword :attr) series-attrs) (map :val series-attrs)))]
                     (compile-observations db ns1 series-id options))))))

(defn compile-observations [db ns1 series-id {release :release}]
  (for [obs (if release  
                (get-obs-by-release db {:release (java-time/sql-timestamp release) :series_id series-id})
                (get-obs db {:series_id series-id}))
          :let [obs-attrs (get-obs-attrs db obs)]]
      (xml/element (xml/qname ns1 "Obs") 
                   (merge {:TIME_PERIOD (str (:time_period obs)) 
                           :OBS_VALUE (:obs_value obs)}  
                          (zipmap (map (comp keyword :attr) obs-attrs) 
                                  (map :val obs-attrs))))))

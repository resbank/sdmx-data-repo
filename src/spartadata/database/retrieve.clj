(ns spartadata.database.retrieve
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [jsonista.core :as json]
            [spartadata.database.upload :refer [validate-data]]
            [spartadata.sdmx-errors :refer [sdmx-error]]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Instanstiate globals


(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")

(def message-counter (atom 0))



;; Define multimethod


(defmulti retrieve-data-message (fn [hdr _ _ _] (get hdr "accept")))

(comment
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
  provided. Returns a clojure.data.xml.node.Element")



;; JSON message


(declare json-data)
(declare json-datasets)
(declare json-series)
(declare json-observations)

(defmethod retrieve-data-message "application/json"
  [_ db {dataflow :flow-ref dimensions :key unused :provider-ref} {validate? :validate :or {validate? false} :as options}]
  (let [datasets (->> (for [agencyid (:agencyid dataflow)] 
                        (for [id (:id dataflow)] 
                          (for [version (:version dataflow)] 
                            [agencyid id version])))
                      (reduce concat)
                      (reduce concat))
        data-message (json-data db datasets dimensions options)]
    (if validate? 
      (println "Validation skipped: JSON validation not supported."))
    {:error (if (instance? clojure.data.xml.node.Element data-message) 
              (-> data-message 
                  zip/xml-zip 
                  (zip-xml/xml1-> ::messg/ErrorMessage)
                  zip/node
                  (get-in [:attrs :code]))
              -1)
     :content-type (if (instance? clojure.data.xml.node.Element data-message)
                     "application/xml"
                     "application/json")
     :content (json/write-value-as-string data-message)}))

(defn json-data [db datasets dimensions options]
  (let [dataset-messages (json-datasets db datasets dimensions options)]
    (if (empty? dataset-messages)
      (sdmx-error 100 (str "No Dataflows exists for query : Target: Dataflow"
                           " - Agency Id: " (clojure.string/join "+" (map first datasets))
                           " - Maintainable Id: " (clojure.string/join "+" (map second datasets))
                           " - Version: " (clojure.string/join "+" (map last datasets))))
      {:Header {:ID (str "IREF" (swap! message-counter inc))
                :Test false
                :Prepared (str (java-time/local-date-time))
                :Sender {:id "SARB.ERD"}
                :Receiver {:id "ANONYMOUS"}}
       :DataSets dataset-messages})))

(defn json-datasets [db datasets dimensions options]
  ;; all/all/all|latest not implemented
  (for [dataset datasets
        :let [[agencyid id version] dataset
              ns1 (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
              dataset-id (get-dataset-id db {:agencyid agencyid :id id :version version})]
        :when dataset-id]
    (let [dataset-attrs (get-dataset-attrs db dataset-id)]
      (merge (zipmap (map (comp keyword :attr) dataset-attrs) (map :val dataset-attrs))
             {:Series (json-series db ns1 dataset-id dimensions options)}))))

(defn json-series [db ns1 dataset-id dimensions options]
  (let [dimension-id-sets (if (not= "all" dimensions) 
                            (for [pos (map inc (range (count dimensions)))
                                  :let [values (nth dimensions (dec pos))]]
                              (map :dimension_id
                                   (if (empty? (first values))
                                     (get-dim-ids-by-pos db (assoc dataset-id :pos pos))
                                     (get-dim-ids-by-vals db (merge dataset-id {:vals values :pos pos}))))))]
    (for [series-id (if (= "all" dimensions) 
                      (map :series_id (get-series-ids db dataset-id))
                      (if (every? (comp not empty?) dimension-id-sets)
                        (reduce #(clojure.set/intersection %1 %2) 
                                (pmap #(into #{} (->> (get-series-ids-from-dim-ids db {:dims %})
                                                      (map vals)
                                                      (reduce concat))) 
                                      dimension-id-sets))))
          :let [series-dims (get-series-dims db {:series_id series-id})
                series-attrs (get-series-attrs db {:series_id series-id})]]
      (merge (zipmap (map (comp keyword :dim) series-dims) (map :val series-dims))
             (zipmap (map (comp keyword :attr) series-attrs) (map :val series-attrs))
             {:Obs (json-observations db ns1 series-id options)}))))

(defn json-observations [db ns1 series-id {release :release}]
  (map (fn [{time-period :time_period obs-value :obs_value attrs :attrs values :vals}] 
         (-> (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
             (assoc :TIME_PERIOD time-period) 
             (assoc :OBS_VALUE obs-value))) 
       (if release  
         (get-obs-by-release db {:release (java-time/sql-timestamp release) :series_id series-id})
         (get-obs db {:series_id series-id}))))



;; SDMX v2.0 message


(declare sdmx-v2-0-data)
(declare sdmx-v2-0-datasets)
(declare sdmx-v2-0-series)
(declare sdmx-v2-0-observations)

(defmethod retrieve-data-message :default
  [_ db {dataflow :flow-ref dimensions :key unused :provider-ref} {validate? :validate :or {validate? false} :as options}]
  (let [datasets (->> (for [agencyid (:agencyid dataflow)] 
                        (for [id (:id dataflow)] 
                          (for [version (:version dataflow)] 
                            [agencyid id version])))
                      (reduce concat)
                      (reduce concat))
        data-message (sdmx-v2-0-data db datasets dimensions options)]
    (if (and validate? (= 1 (count datasets)) (not= "Error" (name (:tag data-message)))) 
      (let [[agencyid id version] (first datasets)]
        (validate-data (str (:sdmx-registry env) "schema/dataflow/" agencyid "/" id "/" version "?format=sdmx-2.0") 
                       (xml/emit-str data-message)))
      (if (and validate? (not= 1 (count datasets))) (println "Validation skipped: Multiple datastructures.")))
    {:error (if (= "Error" (name (:tag data-message))) 
               (-> data-message 
                   zip/xml-zip 
                   (zip-xml/xml1-> ::messg/ErrorMessage)
                   zip/node
                   (get-in [:attrs :code]))
               -1)
      :content-type (if (= "Error" (name (:tag data-message)))
                      "application/xml"
                      "application/vnd.sdmx.compact+xml;version=2.0")
      :content (xml/emit-str data-message)}))

(defn sdmx-v2-0-data [db datasets dimensions options]
  (let [dataset-messages (sdmx-v2-0-datasets db datasets dimensions options)]
    (if (empty? dataset-messages)
      (sdmx-error 100 (str "No Dataflows exists for query : Target: Dataflow"
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

(defn sdmx-v2-0-datasets [db datasets dimensions options]
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
                      (sdmx-v2-0-series db ns1 dataset-id dimensions options)])))))

(defn sdmx-v2-0-series [db ns1 dataset-id dimensions options]
  (let [dimension-id-sets (if (not= "all" dimensions) 
                            (for [pos (map inc (range (count dimensions)))
                                  :let [values (nth dimensions (dec pos))]]
                              (map :dimension_id
                                   (if (empty? (first values))
                                     (get-dim-ids-by-pos db (assoc dataset-id :pos pos))
                                     (get-dim-ids-by-vals db (merge dataset-id {:vals values :pos pos}))))))]
    (for [series-id (if (= "all" dimensions) 
                      (map :series_id (get-series-ids db dataset-id))
                      (if (every? (comp not empty?) dimension-id-sets)
                        (reduce #(clojure.set/intersection %1 %2) 
                                (pmap #(into #{} (->> (get-series-ids-from-dim-ids db {:dims %})
                                                      (map vals)
                                                      (reduce concat))) 
                                      dimension-id-sets))))
          :let [series-dims (get-series-dims db {:series_id series-id})
                series-attrs (get-series-attrs db {:series_id series-id})]]
      (apply xml/element 
             (concat [(xml/qname ns1 "Series") 
                      (merge (zipmap (map (comp keyword :dim) series-dims) (map :val series-dims))
                             (zipmap (map (comp keyword :attr) series-attrs) (map :val series-attrs)))]
                     (sdmx-v2-0-observations db ns1 series-id options))))))

(defn sdmx-v2-0-observations [db ns1 series-id {release :release}]
  (map (fn [{time-period :time_period obs-value :obs_value attrs :attrs values :vals}] 
         (xml/element (xml/qname ns1 "Obs") 
                      (-> (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
                          (assoc :TIME_PERIOD time-period) 
                          (assoc :OBS_VALUE obs-value)))) 
       (if release  
         (get-obs-by-release db {:release (java-time/sql-timestamp release) :series_id series-id})
         (get-obs db {:series_id series-id}))))

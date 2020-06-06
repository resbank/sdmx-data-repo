(ns spartadata.model.retrieve
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [jsonista.core :as json]
            [spartadata.sdmx.errors :refer [sdmx-error]]
            [spartadata.sdmx.validate :refer [validate-data]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Define globals


(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")

(def message-counter (atom 0))



;; Retrieve data message


(declare format-response)
(declare compile-data-message)
(declare format-data-message)
(declare retrieve-datasets)
(declare format-dataset)
(declare retrieve-series)
(declare format-series)
(declare retrieve-observations)
(declare format-observation)

(defn retrieve-data-message
  [db {dataflow :flow-ref dimensions :key unused :provider-ref} {validate? :validate :or {validate? false} :as options}]
  (let [datasets (get-datasets db)
        dataflows (->> (for [agencyid (if (some #(= "all" %) (:agencyid dataflow)) (map :agencyid datasets) (:agencyid dataflow))] 
                         (for [id (if (some #(= "all" %) (:id dataflow)) (map :id datasets) (:id dataflow))] 
                           (for [version (if (some #(= "all" %) (:version dataflow)) (map :version datasets) 
                                           (if (some #(= "latest" %) (:version dataflow)) (:version (first datasets)) (:version dataflow)))] 
                             [agencyid id version])))
                       (reduce concat)
                       (reduce concat)) 
        data-message (compile-data-message db dataflows dimensions options)]
    (if (= "Error" (name (or (:tag data-message) :nil))) 
      (format-response data-message options)
      (if validate? 
        (if (not= 1 (count dataflows))
          (if-let [validation-error (validate-data (first dataflows) (xml/emit-str data-message) options)]
            (format-response validation-error options)
            (format-response data-message options))
          (format-response (sdmx-error 1001 "Validation not supported. Multiple dataflows/structures") options))
        (format-response data-message options)))))

(defmulti format-response (fn [data-message options] (if (= "Error" (name (or (:tag data-message) :nil))) "Error" (:format options))))

(defmethod format-response "Error"
  [data-message options]
  {:error (-> data-message 
              zip/xml-zip 
              (zip-xml/xml1-> ::messg/ErrorMessage)
              zip/node
              (get-in [:attrs :code]))
   :content-type "application/xml"
   :content (xml/emit-str data-message)})

(defmethod format-response "application/json"
  [data-message options]
  {:error 0
   :content-type "application/json"
   :content (json/write-value-as-string data-message)})

(defmethod format-response "application/vnd.sdmx.compact+xml;version=2.0"
  [data-message options]
  {:error 0
   :content-type "application/vnd.sdmx.compact+xml;version=2.0"
   :content (xml/emit-str data-message)})



;; Compile data message


(defn compile-data-message [db dataflows dimensions options]
  (let [dataset-messages (retrieve-datasets db dataflows dimensions options)]
    (if-not (empty? dataset-messages) 
      (format-data-message dataset-messages options) 
      (sdmx-error 100 (str "No data exist for query: Target: Dataflow"
                           " - Agency Id: " (clojure.string/join "+" (map first dataflows))
                           " - Maintainable Id: " (clojure.string/join "+" (map second dataflows))
                           " - Version: " (clojure.string/join "+" (map last dataflows)))))))

(defmulti format-data-message (fn [dataset-messages options] (:format options)))

(defmethod format-data-message "application/json"
  [dataset-messages options]
  {:Header {:ID (str "IREF" (swap! message-counter inc))
            :Test false
            :Prepared (str (java-time/local-date-time))
            :Sender {:id "SARB.ERD"}
            :Receiver {:id "ANONYMOUS"}}
   :DataSets dataset-messages})

(defmethod format-data-message "application/vnd.sdmx.compact+xml;version=2.0"
  [dataset-messages options]
  (xml/element ::messg/CompactData {}
               (xml/element ::messg/Header {}
                            (xml/element ::messg/ID {} (str "IREF" (swap! message-counter inc)))
                            (xml/element ::messg/Test {} false)
                            (xml/element ::messg/Prepared {} (str (java-time/local-date-time)))
                            (xml/element ::messg/Sender {:id "SARB.ERD"})
                            (xml/element ::messg/Receiver {:id "ANONYMOUS"}))
               dataset-messages))



;; Collect datasets


(defn retrieve-datasets [db dataflows dimensions options]
  (for [dataflow dataflows
        :let [[agencyid id version] dataflow
              dataset (get-dataset-and-attrs db {:agencyid agencyid :id id :version version})]
        :when dataset]
    (format-dataset db dataset dimensions (assoc options :dataflow dataflow))))

(defmulti format-dataset (fn [db dataset dimensions options] (:format options)))

(defmethod format-dataset "application/json"
  [db {attrs :attrs values :vals :as dataset} dimensions options]
  (merge (if (first (.getArray attrs)) (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) {})
         {:Series (retrieve-series db dataset dimensions options)}))

(defmethod format-dataset "application/vnd.sdmx.compact+xml;version=2.0"
  [db {attrs :attrs values :vals :as dataset} dimensions {[agencyid id version] :dataflow :as options}]
  (let [ns1 (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")]
    (apply xml/element 
           (concat [(xml/qname ns1 "DataSet") 
                    (if (first (.getArray attrs)) (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) {})
                    (retrieve-series db dataset dimensions (assoc options :ns1 ns1))]))))

(defmethod format-dataset :default
  [db dataset dimensions options]
  (sdmx-error 1002 "Format not supported. Unable to complete request."))



;; Collect series


(defn retrieve-series [db dataset dimensions options]
  (let [dimension-sets (if (not= "all" dimensions) 
                            (for [pos (map inc (range (count dimensions)))
                                  :let [values (nth dimensions (dec pos))]]
                              (map :dimension_id
                                   (if (empty? (first values))
                                     (get-dims-by-pos db (assoc dataset :pos pos))
                                     (get-dims-by-vals db (merge dataset {:vals values :pos pos}))))))]
    (pmap #(format-series db % options)
          (if (= "all" dimensions) 
            (get-series-and-attrs db dataset)
            (if (every? (comp not empty?) dimension-sets)
              (->> (reduce #(clojure.set/intersection %1 %2) 
                           (map #(into #{} (->> (match-series db {:dimension_ids %})
                                                (map vals)
                                                (reduce concat))) 
                                dimension-sets))
                   (hash-map :series_ids)
                   (get-series-and-attrs-from-ids db)))))))

(defmulti format-series (fn [db series options] (:format options)))

(defmethod format-series "application/json"
  [db {series-id :series_id dims :dims dim-values :dim_vals attrs :attrs attr-values :attr_vals} options] 
  (merge (if (first (.getArray attrs)) (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray attr-values))) {})
         (zipmap (mapv keyword (.getArray dims)) (into [] (.getArray dim-values)))
         {:Obs (retrieve-observations db series-id options)}))

(defmethod format-series "application/vnd.sdmx.compact+xml;version=2.0"
  [db {series-id :series_id dims :dims dim-values :dim_vals attrs :attrs attr-values :attr_vals} {ns1 :ns1 :as options}] 
  (apply xml/element 
         (concat [(xml/qname ns1 "Series") 
                  (merge (if (first (.getArray attrs)) (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray attr-values))) {})
                         (zipmap (mapv keyword (.getArray dims)) (into [] (.getArray dim-values))))]
                 (retrieve-observations db series-id options))))



;; Collect observations


(defn retrieve-observations [db series-id {release :release :as options}]
  (map #(format-observation % options) 
       (if release  
         (get-obs-and-attrs-by-release db {:release release :series_id series-id})
         (get-obs-and-attrs db {:series_id series-id}))))

(defmulti format-observation (fn [obs options] (:format options)))

(defmethod format-observation "application/json" 
  [{time-period :time_period obs-value :obs_value attrs :attrs values :vals} _] 
  (-> (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
      (assoc :TIME_PERIOD time-period) 
      (assoc :OBS_VALUE obs-value)))

(defmethod format-observation "application/vnd.sdmx.compact+xml;version=2.0" 
  [{time-period :time_period obs-value :obs_value attrs :attrs values :vals} {ns1 :ns1}] 
  (xml/element (xml/qname ns1 "Obs") 
               (-> (zipmap (mapv keyword (.getArray attrs)) (into [] (.getArray values))) 
                   (assoc :TIME_PERIOD time-period) 
                   (assoc :OBS_VALUE obs-value))))

(ns spartadata.database.upload
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.jdbc :as jdbc]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.sdmx.validate :refer [validate-data]]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Upload data message

(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")
(xml/alias-uri 'struc "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure")

(declare get-components)
(declare upload-dataset)
(declare upload-series)
(declare upload-obs)

;;;; Need a way to ensure that upload data is not run in parallel on the same dataset ;;;;

(defn upload-data-message [db data-message dataflow {validate? :validate release-description :releaseDescription :or {validate? true}}]
  "Usage: (upload-data-message db dimensions dataflow & {:keys [validate?] :or {validate? true}})

  Validate and upload a data message. Duplicate series in the data message will be ignored.

  If release? evaluates to 'true' the upload is considered to be the final version of the data preceding the next release. 
  A description of the data release may be provided"
  (if validate? (validate-data dataflow data-message))
  (with-open [data-message (clojure.java.io/input-stream data-message)] 
    (let [{agencyid :agency-id id :resource-id version :version} dataflow
          data-zipper (-> data-message xml/parse zip/xml-zip)
          components (get-components agencyid id version)
          ns1  (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
          dataset-id (upload-dataset db dataflow (:dataset components) (zip-xml/xml1-> data-zipper (xml/qname ns1 "DataSet")))
          previous-release (get (or (get-latest-release db dataset-id)
                                    (insert-release db (merge {:embargo "0001-01-01T00:00:00" 
                                                               :description "Initial release"} 
                                                              dataset-id)))
                                :embargo)
          timestamp (java-time/sql-timestamp)]
      (if (java-time/after? (java-time/local-date-time previous-release) (java-time/local-date-time))
        (throw (Exception. (str "Next release must follow the previous release chronologically." 
                                "Suggested action: rollback releases until before current time."))))
      (loop [series-zippers (zip-xml/xml-> data-zipper (xml/qname ns1 "DataSet") (xml/qname ns1 "Series"))
             series-uploaded #{}]
        (if series-zippers
          (let [series-zipper (first series-zippers)
                series-id (upload-series db dataset-id (:series components) series-zipper)]
            (if (not (contains? series-uploaded series-id))
              (future (upload-obs db series-id (:obs components) (zip-xml/xml-> series-zipper (xml/qname ns1 "Obs")) previous-release timestamp)))
            (recur (next series-zippers) (conj series-uploaded series-id)))))
      (if release-description
        (insert-release db (merge {:embargo timestamp 
                                   :description release-description} 
                                  dataset-id)))
      "Data upload complete.")))

(defn get-components [agencyid id version]
  (let [components-zipper (-> (slurp (str (:sdmx-registry env) "/sdmxapi/rest/datastructure/" agencyid  "/" id "/" version "?format=sdmx-2.0")) 
                              xml/parse-str 
                              zip/xml-zip
                              (zip-xml/xml1-> ::messg/KeyFamilies ::struc/KeyFamily ::struc/Components))
        dimensions (->> (zip-xml/xml-> components-zipper ::struc/Dimension)
                        (map (fn [loc] (get-in (zip/node loc) [:attrs :conceptRef]))))]
    {:dataset {:attributes  (->> (zip-xml/xml-> components-zipper ::struc/Attribute (zip-xml/attr= :attachmentLevel "DataSet"))
                                 (map (fn [loc] (get-in (zip/node loc) [:attrs :conceptRef])))
                                 (into #{}))}
     :series {:dimensions (into #{} dimensions)
              :dimension-pos (reduce merge (map #(array-map %1 (inc %2)) dimensions (range (count dimensions))))
              :attributes (->> (zip-xml/xml-> components-zipper ::struc/Attribute (zip-xml/attr= :attachmentLevel "Series"))
                               (map (fn [loc] (get-in (zip/node loc) [:attrs :conceptRef])))
                               (into #{}))}
     :obs {:attributes (->> (zip-xml/xml-> components-zipper ::struc/Attribute (zip-xml/attr= :attachmentLevel "Observation"))
                            (map (fn [loc] (get-in (zip/node loc) [:attrs :conceptRef])))
                            (into #{}))}}))

(defn upload-dataset [db dataflow components dataset-zipper]
  (let [dataset (clojure.set/rename-keys dataflow {:agency-id :agencyid :resource-id :id})]
    (jdbc/with-db-transaction [tx db]
      (let [dataset-id (or (get-dataset-id tx dataset)
                           (insert-dataset tx dataset))]
        (doseq [attribute (:attrs (zip/node dataset-zipper))
                :when (contains? (:attributes components) (name (key attribute)))]
          (upsert-dataset-attribute tx (merge {:attr (name (key attribute))
                                               :val (val attribute)}
                                              dataset-id)))
        dataset-id))))

(defn upload-series [db dataset-id components series-zipper]
  (jdbc/with-db-transaction [tx db]
    (let [dimension-ids (for [dimension (:attrs (zip/node series-zipper))
                              :when (contains? (:dimensions components) (name (key dimension)))]
                          (:dimension_id (or (get-dimension-id tx (merge {:dim (name (key dimension))
                                                                          :val (val dimension)}
                                                                         dataset-id))
                                             (insert-dimension tx (merge {:pos (get (:dimension-pos components) 
                                                                                    (name (key dimension)))
                                                                          :dim (name (key dimension))
                                                                          :val (val dimension)}
                                                                         dataset-id)))))
          series-id (or (get-series-id tx (assoc dataset-id :dimension_ids dimension-ids))
                        (insert-series tx (assoc dataset-id :dimension_ids dimension-ids)))]
      (doseq [dimension-id dimension-ids]
        (upsert-series-dimension tx (assoc series-id :dimension_id dimension-id)))
      (doseq [attribute (:attrs (zip/node series-zipper))
              :when (contains? (:attributes components) (name (key attribute)))]
        (upsert-series-attribute tx (merge {:attr (name (key attribute))
                                            :val (val attribute)}
                                           series-id)))
      series-id)))

(defn upload-obs [db series-id components obs-zippers previous-release timestamp]
  (jdbc/with-db-transaction [tx db]
    (let [attrs (map (comp :attrs zip/node) obs-zippers)
          time-periods (map :TIME_PERIOD attrs)
          candidate-obs (zipmap time-periods attrs)
          current-obs (#(select-keys (zipmap (map :time_period %) %) time-periods) (get-obs tx series-id))]
      (doseq [obs (-> (fn [obs time-period {obs-value :OBS_VALUE}] 
                        (let [candidate {:created timestamp
                                         :time_period time-period
                                         :obs_value (Double/parseDouble obs-value)
                                         :series_id (:series_id series-id)}]
                          (if-let [current (get obs time-period)]
                            (if (> (Math/abs (- (:obs_value candidate) (:obs_value current))) (Math/ulp (:obs_value current)))
                              (if (java-time/before? (java-time/local-date-time (:latest_release current)) (java-time/local-date-time previous-release))
                                (assoc obs time-period candidate)
                                (assoc-in obs [time-period :obs_value] (:obs_value candidate)))
                              (dissoc obs time-period))
                            (assoc obs time-period candidate))))
                      (reduce-kv current-obs candidate-obs)
                      vals)]
        (let [obs-id (upsert-obs tx obs)]
          (upsert-obs-attributes tx {:attrs (for [attribute (get candidate-obs (:time_period obs))
                                                  :when (contains? (:attributes components) (name (key attribute)))]
                                              [(name (key attribute)) (val attribute) (:observation_id obs-id)])}))))))

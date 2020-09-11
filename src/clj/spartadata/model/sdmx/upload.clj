(ns spartadata.model.sdmx.upload
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.jdbc :as jdbc]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.sdmx.errors :refer [sdmx-error]]
            [spartadata.sdmx.validate :refer [validate-data]]))



;; Import needed SQL functions


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

(defn upload-data-message [db data-message dataflow {validate? :validate release-description :releaseDescription :or {validate? true} :as options}]
  "Usage: (upload-data-message db dimensions dataflow & {:keys [validate?] :or {validate? true}})

  Validate and upload a data message. Duplicate series in the data message will be ignored.

  If release? evaluates to 'true' the upload is considered to be the final version of the data preceding the next release. 
  A description of the data release may be provided"
  (let [{:keys [agencyid id version]} dataflow]
    (if-let [result (when validate? (validate-data dataflow 
                                                   data-message 
                                                   (assoc options :format "application/vnd.sdmx.compact+xml;version=2.0")))]
      {:error 1000
       :content-type "application/xml"
       :content result}
      (with-open [data-message (clojure.java.io/input-stream data-message)] 
        (let [data-zipper (-> data-message xml/parse zip/xml-zip)
              components (get-components agencyid id version)
              ns1  (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
              dataset-id (upload-dataset db dataflow (:dataset components) (zip-xml/xml1-> data-zipper (xml/qname ns1 "DataSet")))
              previous-release (get (or (get-latest-release db dataset-id)
                                        (insert-release db (merge {:release "0001-01-01T00:00:00" 
                                                                   :description "Initial release"} 
                                                                  dataset-id)))
                                    :release)
              timestamp (java-time/sql-timestamp)]
          (if (java-time/after? (java-time/local-date-time previous-release) (java-time/local-date-time))
            {:error 1003
             :content-type "application/xml"
             :content (xml/emit-str (sdmx-error 1003 "Upload error. Next release must follow the previous release chronologically."))}
            (do (loop [series-zippers (zip-xml/xml-> data-zipper (xml/qname ns1 "DataSet") (xml/qname ns1 "Series"))
                       series-uploaded #{}]
                  (if series-zippers
                    (let [series-zipper (first series-zippers)
                          series-id (upload-series db dataset-id (:series components) series-zipper)]
                      (if (not (contains? series-uploaded series-id))
                        (future (upload-obs db series-id (:obs components) (zip-xml/xml-> series-zipper (xml/qname ns1 "Obs")) previous-release timestamp)))
                      (recur (next series-zippers) (conj series-uploaded series-id)))))
                (if release-description
                  (insert-release db (merge {:release timestamp 
                                             :description release-description} 
                                            dataset-id)))
                {:error 0
                 :content-type "application/xml"
                 :content (xml/emit-str (xml/element :Results {}
                                                     (xml/element :Result {}
                                                                  (xml/element :Text {} "Data uploaded successfully"))))})))))))

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
  (println (contains? (:attributes components) "RELEASE"))
  (let [has_release_attr? (contains? (:attributes components) "RELEASE")
        components (update components :attributes #(clojure.set/difference % #{"RELEASE"}))]
    (jdbc/with-db-transaction [tx db]
      (let [dataset-id (or (get-dataset tx dataflow)
                           (insert-dataset tx (assoc dataflow :has_release_attr has_release_attr?)))]
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
                          (:dimension_id (or (get-dimension tx (merge {:dim (name (key dimension))
                                                                       :val (val dimension)}
                                                                      dataset-id))
                                             (insert-dimension tx (merge {:pos (get (:dimension-pos components) 
                                                                                    (name (key dimension)))
                                                                          :dim (name (key dimension))
                                                                          :val (val dimension)}
                                                                         dataset-id)))))
          series-id (or (match-single-series tx (assoc dataset-id :dimension_ids dimension-ids))
                        (insert-series tx (assoc dataset-id :dimension_ids dimension-ids)))]
      (doseq [dimension-id dimension-ids]
        (upsert-series-dimension tx (assoc series-id :dimension_id dimension-id)))
      (doseq [attribute (:attrs (zip/node series-zipper))
              :when (contains? (:attributes components) (name (key attribute)))]
        (upsert-series-attribute tx (merge {:attr (name (key attribute))
                                            :val (val attribute)}
                                           series-id)))
      series-id)))

(defn upload-obs [db series components obs-zippers previous-release timestamp]
  (jdbc/with-db-transaction [tx db]
    (let [observations (get-obs-and-attrs tx series)
          previous-release (-> (java-time/local-date-time previous-release) (java-time/plus (java-time/millis 1)))]
      (doseq [candidate (map (comp :attrs zip/node) obs-zippers)
              :let [time-period (:TIME_PERIOD candidate)
                    obs-value (Double/parseDouble (:OBS_VALUE candidate))
                    attrs (-> candidate (dissoc :TIME_PERIOD) (dissoc :OBS_VALUE))]]
        (if-let [current (first (filter #(= time-period (:time_period %)) observations))]
          (when (or (> (Math/abs (- obs-value (:obs_value current))) (Math/ulp (:obs_value current)))
                    (not= (zipmap (mapv keyword (.getArray (:attrs current))) (into [] (.getArray (:vals current)))) attrs)
                    (and (java.lang.Double/isNaN obs-value) (not (java.lang.Double/isNaN (:obs_value current))))
                    (and (not (java.lang.Double/isNaN obs-value)) (java.lang.Double/isNaN (:obs_value current))))
            (if (java-time/before? (java-time/local-date-time (:created current)) previous-release)
              ;; Observation has changed and current observation is released => create new observation
              (let [{obs-id :observation_id} (upsert-obs tx {:created timestamp 
                                                             :time_period time-period 
                                                             :obs_value obs-value 
                                                             :series_id (:series_id series)})]
                (upsert-obs-attributes tx {:attrs (for [attr attrs] [(name (key attr)) (val attr) obs-id])}))
              ;; Observation has changed but current observation is not released => update observation 
              (do (upsert-obs tx (assoc current :obs_value obs-value))
                  (upsert-obs-attributes tx {:attrs (for [attr attrs] [(name (key attr)) (val attr) (:observation_id current)])}))))
          ;; Observation does not exist => create new observation
          (let [{obs-id :observation_id} (upsert-obs tx {:created timestamp 
                                                         :time_period time-period 
                                                         :obs_value obs-value 
                                                         :series_id (:series_id series)})]
            (upsert-obs-attributes tx {:attrs (for [attr attrs] [(name (key attr)) (val attr) obs-id])})))))))



;; Upload data message (including historical data)

;;;; Need a way to ensure that upload data is not run in parallel on the same dataset ;;;;

(defn upload-historical-data-message [db data-message dataflow {validate? :validate next-release :releaseDateTime release-description :releaseDescription :or {validate? true} :as options}]
  "Usage: (upload-data-message db dimensions dataflow & {:keys [validate?] :or {validate? true}})

  Validate and upload a data message. Duplicate series in the data message will be ignored.
  
  Historical data is data that contains previously released data that has yet to be uploaded. It requires that the next release be specified, 
  with the further requirement that it must follow chronologically from the previous release - but must be before the current time. A description 
  of the release may also be specified. This function is used for initialising the database, don't use it unless you are sure of how it works."
  (let [{:keys [agencyid id version]} dataflow]
    (if-let [result (when validate? (validate-data dataflow 
                                                   data-message 
                                                   (assoc options :format "application/vnd.sdmx.compact+xml;version=2.0")))]
      {:error 1000
       :content-type "application/xml"
       :content result}
      (with-open [data-message (clojure.java.io/input-stream data-message)]
        (let [data-zipper (-> data-message xml/parse zip/xml-zip)
              components (get-components agencyid id version)
              ns1  (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
              dataset-id (upload-dataset db dataflow (:dataset components) (zip-xml/xml1-> data-zipper (xml/qname ns1 "DataSet")))
              previous-release (get (or (get-latest-release db dataset-id)
                                        (insert-release db (merge {:release "0001-01-01T00:00:00" 
                                                                   :description "Initial release"} 
                                                                  dataset-id)))
                                    :release)]
          (if (= (java-time/local-date-time next-release) (java-time/local-date-time previous-release))
            {:error 1003
             :content-type "application/xml"
             :content (xml/emit-str (sdmx-error 1003 "Upload error. Release already exists."))}
            (if (java-time/after? (java-time/local-date-time previous-release) (java-time/local-date-time next-release))
              {:error 1003
               :content-type "application/xml"
               :content (xml/emit-str (sdmx-error 1003 "Upload error. Upload release date can't precede the release date of the previous release."))}
              (if (get-obs-following-release-by-dataset db (assoc dataset-id :release next-release))
                {:error 1003
                 :content-type "application/xml"
                 :content (xml/emit-str (sdmx-error 1003 "Upload error. Upload release date can't precede the creation date of the existing data."))}
                (do (loop [series-zippers (zip-xml/xml-> data-zipper (xml/qname ns1 "DataSet") (xml/qname ns1 "Series"))
                           series-uploaded #{}]
                      (if series-zippers
                        (let [series-zipper (first series-zippers)
                              series-id (upload-series db dataset-id (:series components) series-zipper)]
                          (if (not (contains? series-uploaded series-id))
                            (future (upload-obs db series-id (:obs components) (zip-xml/xml-> series-zipper (xml/qname ns1 "Obs")) previous-release next-release)))
                          (recur (next series-zippers) (conj series-uploaded series-id)))))
                    (insert-release db (merge {:release next-release 
                                               :description (str release-description)} 
                                              dataset-id))
                    {:error 0
                     :content-type "application/xml"
                     :content (xml/emit-str (xml/element :Results {}
                                                         (xml/element :Result {}
                                                                      (xml/element :Text {} "Data uploaded successfully"))))})))))))))

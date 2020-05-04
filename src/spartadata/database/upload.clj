(ns spartadata.database.upload
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time])
  (:import [javax.xml XMLConstants transform.Source transform.stream.StreamSource validation.SchemaFactory validation.Schema validation.Validator]
           [org.xml sax.SAXException]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Upload data message

(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")
(xml/alias-uri 'struc "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure")

(declare validate-data)
(declare get-components)
(declare upload-dataset)
(declare upload-series)
(declare upload-obs)

;;;; Need a way to ensure that upload data is not run in parallel on the same dataset ;;;;

(defn upload-data-message [db data-message {agencyid :agency-id id :resource-id version :version} {:keys [validate? release? release-description] :or {validate? true release? false}}]
  "Usage: (upload-data-message db dimensions dataflow & {:keys [validate?] :or {validate? true}})

  Validate and upload a data message. Duplicate series in the data message will be ignored.

  If release? evaluates to 'true' the upload is considered to be the final version of the data preceding the next release. 
  A description of the data release may be provided"
  (with-open [data-message (clojure.java.io/input-stream data-message)]
    (if validate? (validate-data (str (:sdmx-registry env) "/sdmxapi/rest/schema/dataflow/" agencyid  "/" id "/" version "?format=sdmx-2.0") data-message)))
  (with-open [data-message (clojure.java.io/input-stream data-message)] 
    (let [data-zipper (-> data-message xml/parse zip/xml-zip)
          components (get-components agencyid id version)
          ns1  (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
          dataset-id (upload-dataset db {:agencyid agencyid :id id :version version} (:dataset components) (zip-xml/xml1-> data-zipper (xml/qname ns1 "DataSet")))
          previous-release (get (or (get-latest-release db dataset-id)
                                    (insert-release db (merge {:embargo (java-time/sql-timestamp "0001-01-01T00:00:00") 
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
      (if release?
        (insert-release db (merge {:embargo timestamp 
                                   :description (str release-description)} 
                                  dataset-id)))
      nil)))

(defn validate-data [schema data-message]
  (with-open [message-schema (clojure.java.io/input-stream  "resources\\sdmx\\v2_0\\SDMXMessage.xsd")
              data-message (if (string? data-message) 
                             (clojure.java.io/reader (char-array data-message))
                             (clojure.java.io/input-stream data-message))]
    (-> XMLConstants/W3C_XML_SCHEMA_NS_URI
        (SchemaFactory/newInstance)
        (.newSchema (into-array [(StreamSource. message-schema) (StreamSource. schema)]))
        (.newValidator)
        (.validate (StreamSource. data-message)))))

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

(defn upload-dataset [db dataset components dataset-zipper]
  (jdbc/with-db-transaction [tx db]
    (let [dataset-id (or (get-dataset-id tx dataset)
                         (insert-dataset tx dataset))]
      (doseq [attribute (:attrs (zip/node dataset-zipper))
              :when (contains? (:attributes components) (name (key attribute)))]
        (upsert-dataset-attribute tx (merge {:attr (name (key attribute))
                                             :val (val attribute)}
                                            dataset-id)))
      dataset-id)))

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

(defn- upload-obs [db series-id components obs-zippers previous-release timestamp]
  (jdbc/with-db-transaction [tx db]
    (doseq [obs-zipper obs-zippers
            :let [attributes  (:attrs (zip/node obs-zipper))
                  time-period (java-time/sql-date (:TIME_PERIOD attributes))
                  obs-value (Double/parseDouble (:OBS_VALUE attributes))
                  obs-id (if-let [obs (get-live-obs tx (merge series-id {:time_period time-period}))]
                           (if (> (Math/abs (- obs-value (:obs_value obs))) (Math/ulp (:obs_value obs)))
                             (if (:antecedent (created-previous-to? tx (assoc obs :release previous-release)))
                               (do (kill-obs tx obs)
                                   (insert-obs tx (merge {:created timestamp
                                                          :time_period time-period
                                                          :obs_value obs-value}
                                                         series-id)))
                               (update-obs tx (assoc obs :obs_value obs-value)))
                             obs)
                           (insert-obs tx (merge {:created timestamp
                                                  :time_period time-period
                                                  :obs_value obs-value}
                                                 series-id)))]]
      (doseq [attribute attributes
              :when (contains? (:attributes components) (name (key attribute)))]
        (upsert-obs-attribute tx (merge {:attr (name (key attribute))
                                         :val (val attribute)}
                                        obs-id))))))

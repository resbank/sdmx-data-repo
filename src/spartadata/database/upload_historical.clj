(ns spartadata.database.upload-historical
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.jdbc :as jdbc]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.database.upload :refer [validate-data get-components upload-dataset upload-series]]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Upload data message (including historical data)

;;;; Need a way to ensure that upload data is not run in parallel on the same dataset ;;;;

(declare upload-historical-obs)

(defn upload-historical-data-message [db data-message {agencyid :agency-id id :resource-id version :version} {:keys [validate? nextRelease releaseDescription] :or {validate? true}}]
  "Usage: (upload-data-message db dimensions dataflow & {:keys [validate?] :or {validate? true}})

  Validate and upload a data message. Duplicate series in the data message will be ignored.
  
  Historical data is data that contains previously released data that has yet to be uploaded. It requires that the next release be specified, 
  with the further requirement that it must follow chronologically from the previous release - but must be before the current time. A description 
  of the release may also be specified. This function is used for initialising the database, don't use it unless you are sure of how it works."
  (if validate? (validate-data (str (:sdmx-registry env) "/sdmxapi/rest/schema/dataflow/" agencyid  "/" id "/" version "?format=sdmx-2.0") data-message))
  (with-open [data-message (clojure.java.io/input-stream data-message)]
    (let [data-zipper (-> data-message xml/parse zip/xml-zip)
          components (get-components agencyid id version)
          ns1  (str "urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=" agencyid ":" id "(" version "):compact")
          next-release (java-time/local-date-time nextRelease)
          dataset-id (upload-dataset db {:agencyid agencyid :id id :version version} (:dataset components) (zip-xml/xml1-> data-zipper (xml/qname ns1 "DataSet")))
          previous-release (get (or (get-latest-release db dataset-id)
                                    (insert-release db (merge {:embargo (java-time/sql-timestamp "0001-01-01T00:00:00") 
                                                               :description "Initial release"} 
                                                              dataset-id)))
                                :embargo)]
      (if (= next-release (java-time/local-date-time previous-release))
        (println "Warning, release already exists. No data was uploaded")
        (do
          (if (java-time/after? next-release (java-time/local-date-time))
            (throw (Exception. ("This is an historical-data upload, next release must be some time in the past."))))
          (if (java-time/after? (java-time/local-date-time previous-release) next-release)
            (throw (Exception.  "Next release must follow the previous release chronologically.")))
          (loop [series-zippers (zip-xml/xml-> data-zipper (xml/qname ns1 "DataSet") (xml/qname ns1 "Series"))
                 series-uploaded #{}]
            (if series-zippers
              (let [series-zipper (first series-zippers)
                    series-id (upload-series db dataset-id (:series components) series-zipper)]
                (if (not (contains? series-uploaded series-id))
                  (future (upload-historical-obs db series-id (:obs components) (zip-xml/xml-> series-zipper (xml/qname ns1 "Obs")) previous-release next-release)))
                (recur (next series-zippers) (conj series-uploaded series-id)))))
          (insert-release db (merge {:embargo (java-time/sql-timestamp next-release) 
                                     :description (str releaseDescription)} 
                                    dataset-id))
          nil)))))

(defn- upload-historical-obs [db series-id components obs-zippers previous-release next-release]
  (jdbc/with-db-transaction [tx db]
    (doseq [obs-zipper obs-zippers
            :let [attributes  (:attrs (zip/node obs-zipper))
                  time-period (java-time/sql-date (:TIME_PERIOD attributes))
                  obs-value (Double/parseDouble (:OBS_VALUE attributes))
                  timestamp (java-time/sql-timestamp next-release )
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

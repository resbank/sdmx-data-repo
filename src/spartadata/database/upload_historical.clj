(ns spartadata.database.upload-historical
  (:require [clojure.data.xml :as xml]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.java.jdbc :as jdbc]
            [clojure.zip :as zip]
            [environ.core :refer [env]]
            [hugsql.core :as sql]
            [java-time]
            [spartadata.database.upload :refer [get-components upload-dataset upload-series upload-obs]]
            [spartadata.sdmx.validate :refer [validate-data]]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")



;; Upload data message (including historical data)

;;;; Need a way to ensure that upload data is not run in parallel on the same dataset ;;;;

(defn upload-historical-data-message [db data-message dataflow {validate? :validate next-release :release release-description :releaseDescription :or {validate? true}}]
  "Usage: (upload-data-message db dimensions dataflow & {:keys [validate?] :or {validate? true}})

  Validate and upload a data message. Duplicate series in the data message will be ignored.
  
  Historical data is data that contains previously released data that has yet to be uploaded. It requires that the next release be specified, 
  with the further requirement that it must follow chronologically from the previous release - but must be before the current time. A description 
  of the release may also be specified. This function is used for initialising the database, don't use it unless you are sure of how it works."
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
                                :embargo)]
      (if (= (java-time/local-date-time next-release) (java-time/local-date-time previous-release))
        (println "Warning, release already exists. No data was uploaded")
        (do
          (if (java-time/after? (java-time/local-date-time next-release) (java-time/local-date-time))
            (throw (Exception. "This is an historical-data upload, next release must be some time in the past.")))
          (if (java-time/after? (java-time/local-date-time previous-release) (java-time/local-date-time next-release))
            (throw (Exception.  "Next release must follow the previous release chronologically.")))
          (loop [series-zippers (zip-xml/xml-> data-zipper (xml/qname ns1 "DataSet") (xml/qname ns1 "Series"))
                 series-uploaded #{}]
            (if series-zippers
              (let [series-zipper (first series-zippers)
                    series-id (upload-series db dataset-id (:series components) series-zipper)]
                (if (not (contains? series-uploaded series-id))
                  (future (upload-obs db series-id (:obs components) (zip-xml/xml-> series-zipper (xml/qname ns1 "Obs")) previous-release next-release)))
                (recur (next series-zippers) (conj series-uploaded series-id)))))
          (insert-release db (merge {:embargo next-release 
                                     :description (str release-description)} 
                                    dataset-id))
          "Historical data upload complete.")))))

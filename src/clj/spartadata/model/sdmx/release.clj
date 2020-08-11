(ns spartadata.model.sdmx.release
  (:require [clojure.data.xml :as xml] 
            [hugsql.core :as sql]
            [java-time]
            [spartadata.sdmx.errors :refer [sdmx-error]]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")

(declare get-dataset)
(declare insert-release)



;; Rollback release


(defn add-release [db dataflow {description :releaseDescription next-release :releaseDateTime}]
  (if-let [dataset (get-dataset db dataflow)]
    (let [previous-release (:release (get-latest-release db dataset))
          next-release (or next-release (java-time/local-date-time))]
      (if (or (= (java-time/local-date-time next-release) 
                 (java-time/local-date-time previous-release))
              (java-time/before? (java-time/local-date-time next-release) 
                                 (java-time/local-date-time previous-release)))
        {:error 1003
         :content-type "application/xml"
         :content (sdmx-error 1003 (str "Upload error. Release date (" 
                                        next-release ") can't precede the previous release ("
                                        previous-release ")."))}
        (try (insert-release db
                             (-> dataset
                                 (assoc :description description)
                                 (assoc :release next-release)))
             {:error 0
              :content-type "text/plain"
              :content (str "Dataset released with description: " (:description description) ".")}
             (catch Exception e (.printStackTrace e) 
               {:error 1003
                :content-type "application/xml"
                :content (sdmx-error 1003 (str "Upload error." (.getMessage e)))}))))
    {:error 0
     :content-type "application/xml"
     :content (sdmx-error 1003 (str "Upload error. Release not added, could not find dataset: " dataflow "."))})) 

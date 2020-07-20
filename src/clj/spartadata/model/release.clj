(ns spartadata.model.release
  (:require [hugsql.core :as sql]
            [java-time]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")

(declare get-dataset)
(declare insert-release)



;; Rollback release


(defn add-release [db dataflow description]
  (->> dataflow
       (get-dataset db)
       (merge description)
       (merge {:release (java-time/sql-timestamp)})
       (insert-release db))
  {:error 0
   :content-type "text/plain"
   :content (str "Dataset released with description: " (:description description) ".")}) 

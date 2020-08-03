(ns spartadata.model.sdmx.delete
  (:require [clojure.java.jdbc :as jdbc]
            [hugsql.core :as sql]))



;; Import needed SQL functions


(sql/def-db-fns "sql/delete.sql")
(sql/def-db-fns "sql/query.sql")

(declare tx)
(declare get-dataset)
(declare delete-series-by-dataset-id)
(declare delete-dataset-by-dataset-id)



;; Delete dataset


(defn delete-dataset [db dataset]
  (if-let [dataset-id (get-dataset db dataset)]
    (jdbc/with-db-transaction [tx db]
      (delete-series-by-dataset-id tx dataset-id)
      (delete-dataset-by-dataset-id tx dataset-id)))
  {:error 0
   :content-type "text/plain"
   :content "Dataset deleted."})

(ns spartadata.database.delete
  (:require [clojure.java.jdbc :as jdbc]
            [hugsql.core :as sql]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/delete.sql")
(sql/def-db-fns "sql/query.sql")



;; Delete dataset


(defn delete-dataset [db dataset]
  (if-let [dataset-id (get-dataset-id db dataset)]
    (jdbc/with-db-transaction [tx db]
      (delete-series-by-dataset-id tx dataset-id)
      (delete-dataset-by-dataset-id tx dataset-id))))

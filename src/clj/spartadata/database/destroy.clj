(ns spartadata.database.destroy
  (:require [clojure.java.jdbc :as jdbc]
            [environ.core :refer [env]]
            [hikari-cp.core :refer [make-datasource close-datasource]]
            [hugsql.core :as sql]))



;; Define table creation functions for sdmx database


(sql/def-db-fns "sql/tables.sql")
;(sql/def-db-fns "sql/functions.sql")
;(sql/def-db-fns "sql/triggers.sql")
(sql/def-db-fns "sql/index.sql")



;; Initialise and rollback database

(defn destroy []
  (let [db {:connection-uri (:sdmx-postgres env)}]
    (jdbc/with-db-transaction [tx db]
      (drop-array-idx tx)
      (drop-obs-idx tx)
      (drop-obs-attr-idx tx)
      (drop-usr-log-table tx)
      (drop-dataset-log-table tx)
      (drop-provider-table tx)
      (drop-role-table tx)
      (drop-authentication-table tx)
      (drop-observation-attr-table tx)
      (drop-observation-table tx)
      (drop-series-attr-table tx)
      (drop-series-dimension-table tx)
      (drop-series-table tx)
      (drop-dimension-table tx)
      (drop-release-table tx)
      (drop-dataset-attr-table tx)
      (drop-dataset-table tx))))




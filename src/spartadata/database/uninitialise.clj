(ns spartadata.database.uninitialise
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

(defn rollback []
  (let [db {:datasource (make-datasource {:jdbc-url (:db env)})}]
    (jdbc/with-db-transaction [tx db]
      (drop-array-idx tx)
      (drop-living-idx tx)
      (drop-observation-attr-table tx)
      (drop-observation-table tx)
      (drop-series-attr-table tx)
      (drop-series-dimension-table tx)
      (drop-series-table tx)
      (drop-dimension-table tx)
      (drop-release-table tx)
      (drop-dataset-attr-table tx)
      (drop-dataset-table tx))
    (close-datasource (:datasource db))))

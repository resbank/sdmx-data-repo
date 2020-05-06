(ns spartadata.database.initialise
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


(defn init []
  (let [db {:datasource (make-datasource {:jdbc-url (:sdmx-postgres env)})}]
    (jdbc/with-db-transaction [tx db]
      (create-dataset-table tx)
      (create-dataset-attr-table tx)
      (create-release-table tx)
      (create-dimension-table tx)
      (create-series-table tx)
      (create-series-dimension-table tx)
      (create-series-attr-table tx)
      (create-observation-table tx)
      (create-observation-attr-table tx)
      (create-living-idx tx)
      (create-array-idx tx))
    (close-datasource (:datasource db))))

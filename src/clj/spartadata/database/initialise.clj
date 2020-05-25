(ns spartadata.database.initialise
  (:require [clojure.java.jdbc :as jdbc]
            [environ.core :refer [env]]
            [hugsql.core :as sql]))



;; Define table creation functions for sdmx database


(sql/def-db-fns "sql/tables.sql")
(sql/def-db-fns "sql/functions.sql")
;(sql/def-db-fns "sql/triggers.sql")
(sql/def-db-fns "sql/index.sql")



;; Initialise and rollback database


(defn initialise []
  (let [db {:connection-uri (:sdmx-postgres env)}]
    (jdbc/with-db-transaction [tx db]
      ;(create-intarray-extension tx)
      (create-dataset-table tx)
      (create-dataset-attr-table tx)
      (create-release-table tx)
      (create-dimension-table tx)
      (create-series-table tx)
      (create-series-dimension-table tx)
      (create-series-attr-table tx)
      (create-observation-table tx)
      (create-observation-attr-table tx)
      (create-obs-attr-idx tx)
      (create-obs-idx tx)
      (create-array-idx tx))))

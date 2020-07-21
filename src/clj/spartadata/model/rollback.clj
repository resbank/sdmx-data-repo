(ns spartadata.model.rollback
  (:require [clojure.java.jdbc :as jdbc]
            [hugsql.core :as sql]
            [java-time]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")
(sql/def-db-fns "sql/delete.sql")


;; Rollback release


(defn rollback-release [db dataflow]
  "Rolls back the latest realease"
  (let [dataset (get-dataset db dataflow)
        releases (get-releases db dataset)]
    (if (< 1 (count releases))
      (do (doseq [series (get-series db dataset)]
            (jdbc/with-db-transaction [tx db]
              (doseq [obs (get-obs-following-release tx (merge (second releases) series))
                      :when obs] 
                (delete-obs tx obs))))
          (delete-release db (merge (first releases) dataset))
          "Rollback completed.")
      "Nothing to roll back to. No action taken.")))

(ns spartadata.database.rollback
  (:require [clojure.java.jdbc :as jdbc]
            [hikari-cp.core :refer [make-datasource close-datasource]]
            [hugsql.core :as sql]
            [java-time]))



;; Define functions needed in order to migrate jcMod data to SDMX database


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")


;; Rollback release


(defn rollback-release []
  "Rolls back the latest realease
    1. Delete all observations created on or after latest release
    2. Set all observations contained in the release preceding the latest release to valid
    3. Delete latest release from release table")

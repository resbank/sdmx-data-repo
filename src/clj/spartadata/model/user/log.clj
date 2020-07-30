(ns spartadata.model.user.log
  (:require [clojure.java.jdbc :as jdbc]
            [hugsql.core :as sql]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")
(sql/def-db-fns "sql/delete.sql")

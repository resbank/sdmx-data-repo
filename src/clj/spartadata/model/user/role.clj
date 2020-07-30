(ns spartadata.model.user.role
  (:require [clojure.java.jdbc :as jdbc]
            [hugsql.core :as sql]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")
(sql/def-db-fns "sql/delete.sql")



;; User data provider functions


(defn get-roles [db request])

(defn get-role [db request])

(defn add-role [db request])

(defn remove-role [db request])

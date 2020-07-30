(ns spartadata.model.user.provider
  (:require [clojure.java.jdbc :as jdbc]
            [hugsql.core :as sql]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")
(sql/def-db-fns "sql/delete.sql")



;; User data provider functions


(defn get-providers [db request])

(defn get-provider [db request])

(defn add-provider [db request])

(defn remove-provider [db request])

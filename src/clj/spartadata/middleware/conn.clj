(ns spartadata.middleware.conn
  (:require [reitit.middleware :as middleware]))


(defn wrap-connection-pool [handler connection-pool]
  (fn [request]
    (-> request
        (assoc :conn connection-pool)
        handler)))

(def conn
  (middleware/map->Middleware
    {:name ::conn
     :description "Database connection pool middleware."
     :wrap #(wrap-connection-pool %1 %2)}))

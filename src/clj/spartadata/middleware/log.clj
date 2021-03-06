(ns spartadata.middleware.log
  (:require [clojure.string :as string]
            [hugsql.core :as sql]
            [reitit.middleware :as middleware]))


;; Import needed SQL functions


(sql/def-db-fns "sql/update.sql")

(declare insert-data-log-entry)
(declare insert-usr-log-entry)


;; Middleware


(defn get-data-action [context {uri :uri method :request-method}]
  (cond (and (re-find (re-pattern (str #"^" context #"/sdmxapi/modify/data/.+")) uri) 
             (= :put method))
        {:action "create"}  

        (and (re-find (re-pattern (str #"^" context #"/sdmxapi/modify/data/.+/historical$")) uri) 
             (= :post method))
        {:action "hist_upload"}

        (and (re-find (re-pattern (str #"^" context #"/sdmxapi/modify/data/.+/rollback$")) uri) 
             (= :post method))
        {:action "rollboack"}

        (and (re-find (re-pattern (str #"^" context #"/sdmxapi/modify/data/.+")) uri) 
             (= :post method))
        {:action "upload"}

        (and (re-find (re-pattern (str #"^" context #"/sdmxapi/modify/data/.+")) uri) 
             (= :delete method))
        {:action "delete"}

        (and (re-find (re-pattern (str #"^" context #"/sdmxapi/release/data/.+")) uri) 
             (= :post method))
        {:action "release"}))



(defn wrap-log-data-change [handler context]
  (fn [{connection :conn {{flow-ref :strict-flow-ref} :path} :parameters user :identity :as request}]
    (if-let [action (get-data-action context request)]
      (let [response (handler request)]
        (when (= 200 (:status response))
          (insert-data-log-entry {:datasource connection}
                                 (merge action 
                                        user 
                                        (zipmap [:agencyid :id :version] 
                                                (string/split flow-ref #",")))))
        response)
      (handler request))))

(defn get-user-action [context {uri :uri method :request-method}]
  (cond (and (re-find (re-pattern (str #"^" context #"/userapi/user/[^/]+/provider/.+")) uri) 
             (or (= :put method) (= :delete method)))
        {:action "modify_provider"}

        (and (re-find (re-pattern (str #"^" context #"/userapi/user/[^/]+/role/.+")) uri) 
             (or (= :put method) (= :delete method)))
        {:action "modify_role"}
        
        (and (re-find (re-pattern (str #"^" context #"/userapi/user/.+")) uri) 
             (= :put method))
        {:action "create"}  

        (and (re-find (re-pattern (str #"^" context #"/userapi/user/.+")) uri) 
             (= :post method))
        {:action "update"}

        (and (re-find (re-pattern (str #"^" context #"/userapi/user/.+")) uri) 
             (= :delete method))
        {:action "delete"}))



(defn wrap-log-user-change [handler context]
  (fn [{connection :conn {{username :username} :path} :parameters user :identity :as request}]
    (if-let [action (get-user-action context request)]
      (let [response (handler request)]
        (when (= 200 (:status response))
          (insert-usr-log-entry {:datasource connection}
                                (-> action
                                    (assoc :admin_username (:username user))
                                    (assoc :target_usr_username username))))
        response)
      (handler request))))

;; Reitit middleware

(def data-change
  (middleware/map->Middleware
    {:name ::datalog
     :description "Log change to SDMX data"
     :wrap #(wrap-log-data-change %1 %2)}))

(def user-change
  (middleware/map->Middleware
    {:name ::userlog
     :description "Log change to Sparta Data user"
     :wrap #(wrap-log-user-change %1 %2)}))

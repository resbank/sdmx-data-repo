(ns spartadata.model.user.role
  (:require [clojure.data.xml :as xml]
            [clojure.string :as string]
            [hugsql.core :as sql]
            [spartadata.sdmx.errors :refer [sdmx-error]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")
(sql/def-db-fns "sql/delete.sql")

(declare get-dataset)
(declare get-user)
(declare get-roles)
(declare get-role)
(declare insert-role)
(declare delete-role)



;; User data set role functions


(defmulti format-roles 
  (fn [content-type _ _]
    (when content-type
      (-> content-type
          (clojure.string/replace #";" "_")
          (clojure.string/replace #"=" "-")
          keyword))))

(defmethod format-roles :default
  [_ {username :username}  roles]
  (if username
    (if (first roles)
      {:error 0
       :content {:username username
                 :roles (map (fn [role]
                               {:role (:role role)
                                :dataset (dissoc role :role)}) 
                             roles)}}
      {:error 100
       :content {:error {:code 100
                         :errormessage "Not found. No roles returned for user."}}})
    {:error 100
     :content {:error {:code 100
                       :errormessage "Not found. User not found."}}}))

(defmethod format-roles :application/xml
  [_ {username :username} roles]
  (if username
    (if (first roles)
      {:error 0
       :content-type "application/xml"
       :content (->> (map (fn [role]
                            (xml/element :role {:ROLE (:role role)}
                                         (xml/element :DataSet
                                                      (xml/element :AgencyID {} (:agencyid role))
                                                      (xml/element :ID {} (:id role))
                                                      (xml/element :Version {} (:version role)))))
                          roles)
                     (xml/element :Roles {:UserName username})
                     (xml/emit-str))}
      {:error 100
       :content-type "application/xml"
       :content (xml/emit-str (sdmx-error 100 "Not found. No roles returned for user."))})
    {:error 100
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 100 "Not found. User not found."))}))

(defn retrieve-roles 
  [db content-type user]
  (if-let [user (get-user db user)]
    (format-roles content-type 
                  user
                  (get-roles db user))
    (format-roles content-type 
                  nil
                  [nil])))



  ;; User data set role modification functions

  (defmulti format-modify-role
    (fn [content-type _]
      (when content-type
        (-> content-type
            (clojure.string/replace #";" "_")
            (clojure.string/replace #"=" "-")
            keyword))))

  (defmethod format-modify-role :default
    [_ result]
    (if (= 1 result)
      {:error 0
       :content {:success {:successmessage "Role successfully updated."}}}
      {:error 1006
       :content {:error {:code 1006
                         :errormessage "Role update failed."}}}))

  (defmethod format-modify-role :application/xml
    [_ result]
    (if (= 1 result)
      {:error 0
       :content-type "application/xml"
       :content (->> (xml/element :Success {}
                                  (xml/element :SuccessMessage {} 
                                               "Role successfully updated."))
                     (xml/emit-str))}
      {:error 1006
       :content-type "application/xml"
       :content (xml/emit-str (sdmx-error 1006 "Role update failed."))}))

  (defn create-role 
    [db content-type user role dataflow]
    (let [user (get-user db user)
          dataset (get-dataset db dataflow)]
      (if (and user dataset)
        (if-not (get-role db (merge user role dataset))
          (format-modify-role content-type
                              (insert-role db (merge user 
                                                     role
                                                     dataset)))
          (format-modify-role content-type 0))
        (format-modify-role content-type 0))))

  (defn remove-role 
    [db content-type user role dataflow]
    (let [user (get-user db user)
          dataset (get-dataset db dataflow)]
      (if (and user dataset)
        (format-modify-role content-type 
                            (delete-role db (merge user
                                                   role
                                                   dataset)))
        (format-modify-role content-type 0))))

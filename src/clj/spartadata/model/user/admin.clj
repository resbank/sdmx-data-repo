(ns spartadata.model.user.admin
  (:require [clojure.data.xml :as xml]
            [clojure.string :as string]
            [hugsql.core :as sql]
            [spartadata.sdmx.errors :refer [sdmx-error]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")
(sql/def-db-fns "sql/delete.sql")

(declare delete-user)
(declare get-users)
(declare get-user)
(declare upsert-user)
(declare get-providers)
(declare get-roles)



;; SDMX type hierarchy


(derive :application/vnd.sdmx.compact+xml_version-1.0 :application/xml)
(derive :application/vnd.sdmx.compact+xml_version-2.0 :application/xml)
(derive :application/vnd.sdmx.compact+xml_version-2.1 :application/xml)



;; User query functions

(defmulti format-users 
  (fn [content-type _]
    (-> content-type
        (clojure.string/replace #";" "_")
        (clojure.string/replace #"=" "-")
        keyword)))

(defmethod format-users :application/json
  [_ users]
  (if (first users)
    {:error 0
     :content {:users (map #(-> %
                                (dissoc :user_id)
                                (dissoc :password)) 
                           users)}}
    {:error 100
     :content {:error {:code 100
                       :errormessage "Not found. No users returned."}}}))

(defmethod format-users :application/xml
  [_ users]
  (if (first users)
    {:error 0
     :content-type "application/xml"
     :content (->> (map (fn [user]
                          (xml/element :User {}
                                       (xml/element :Username {} (:username user))
                                       (xml/element :Firstname {} (:firstname user))
                                       (xml/element :Lastname {} (:lastname user))
                                       (xml/element :Email {} (:email user))
                                       (xml/element :Administrator {} (:is_admin user))))
                        users)
                   (xml/element :Users {})
                   (xml/emit-str))}
    {:error 100
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 100 "Not found. No users returned."))}))

(defn retrieve-all-users [db content-type]
  (format-users content-type (get-users db)))

(defn retrieve-self [content-type user]
  (format-users content-type [user]))

(defn retrieve-user [db content-type user]
  (format-users content-type [(get-user db user)]))

;; User modification functions

(defmulti format-modify-user
  (fn [content-type _]
    (-> content-type
        (clojure.string/replace #";" "_")
        (clojure.string/replace #"=" "-")
        keyword)))

(defmethod format-modify-user :application/json
  [_ result]
  (if (= 1 result)
    {:error 0
     :content {:success {:successmessage "User successfully updated."}}}
    {:error 1006
     :content {:error {:code 1006
                       :errormessage "User update failed."}}}))

(defmethod format-modify-user :application/xml
  [_ result]
  (if (= 1 result)
    {:error 0
     :content-type "application/xml"
     :content (->> (xml/element :Success {}
                                (xml/element :SuccessMessage {} 
                                             "User successfully updated."))
                   (xml/emit-str))}
    {:error 1006
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 1006 "User update failed."))}))


(defn update-self [db content-type user user-fields]
  (format-modify-user content-type 
                      (upsert-user db (merge user user-fields))))

(defn create-user [db content-type user user-fields]
  (if-not (get-user db user)
    (format-modify-user content-type
                        (upsert-user db (merge user 
                                               user-fields)))
    (format-modify-user content-type 0)))

(defn update-user [db content-type user user-fields]
  (if (get-user db user)
    (format-modify-user content-type
                        (upsert-user db (merge user 
                                               user-fields)))
    (format-modify-user content-type 0)))

(defn remove-user [db content-type user]
  (format-modify-user content-type 
                      (delete-user db (merge user))))

;; User profiles


(defmulti format-profile 
  (fn [content-type _ _ _]
    (-> content-type
        (clojure.string/replace #";" "_")
        (clojure.string/replace #"=" "-")
        keyword)))

(defmethod format-profile :application/json
  [_ user providers roles]
  (if user
    {:error 0
     :content {:user (-> user
                         (dissoc :user_id)
                         (dissoc :password))
               :providers (map #(-> %
                                    (dissoc :user_id)
                                    (dissoc :provider_id)) 
                               providers)
               :roles (map (fn [role]
                             {:role (:role role)
                              :dataset (dissoc role :role)}) 
                           roles)}}
    {:error 100
     :content {:error {:code 100
                       :errormessage "Not found. No user returned."}}}))

(defmethod format-profile :application/xml
  [_ user providers roles]
  (if user
    {:error 0
     :content-type "application/xml"
     :content (-> (xml/element :UserProfile {}
                               (xml/element :User {}
                                            (xml/element :Username {} (:username user))
                                            (xml/element :Firstname {} (:firstname user))
                                            (xml/element :Lastname {} (:lastname user))
                                            (xml/element :Email {} (:email user))
                                            (xml/element :Administrator {} (:is_admin user)))
                               (map (fn [provider]
                                      (xml/element :Provider {}
                                                   (xml/element :AgencyID {} (:agencyid provider))
                                                   (xml/element :ID {} (:id provider))))
                                    providers)
                               (map (fn [role]
                                      (xml/element :role {:ROLE (:role role)}
                                                   (xml/element :DataSet
                                                                (xml/element :AgencyID {} (:agencyid role))
                                                                (xml/element :ID {} (:id role))
                                                                (xml/element :Version {} (:version role)))))
                                    roles))
                  xml/emit-str)}
    {:error 100
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 100 "Not found. No user returned."))}))


(defn retrieve-self-profile [db content-type user]
  (let [providers (get-providers db user)
        roles (get-roles db user)]
    (format-profile content-type user providers roles)))

(defn retrieve-profile [db content-type user]
  (let [user (get-user db user)
        providers (get-providers db user)
        roles (get-roles db user)]
    (format-profile content-type user providers roles)))

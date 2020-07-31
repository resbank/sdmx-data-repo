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

(defn retrieve-self-profile [content-type user]
  (format-users content-type [user]))

(defn retrieve-profile [db content-type user]
  (format-users content-type [(get-user db user)]))

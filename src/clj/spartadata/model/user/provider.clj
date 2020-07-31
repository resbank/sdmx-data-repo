(ns spartadata.model.user.provider
  (:require [clojure.data.xml :as xml]
            [clojure.string :as string]
            [hugsql.core :as sql]
            [spartadata.sdmx.errors :refer [sdmx-error]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")
(sql/def-db-fns "sql/update.sql")
(sql/def-db-fns "sql/delete.sql")

(declare get-user)
(declare get-providers)
(declare get-providers-by-user)
(declare get-provider)
(declare insert-provider)
(declare delete-provider)

;; User data provider functions


(defmulti format-providers 
  (fn [content-type _ _]
    (-> content-type
        (clojure.string/replace #";" "_")
        (clojure.string/replace #"=" "-")
        keyword)))

(defmethod format-providers :application/json
  [_ {username :username} providers]
  (if username
    (if (first providers)
      {:error 0
       :content {:providers {:username username
                             :provider (map #(-> %
                                                 (dissoc :provider_id) 
                                                 (dissoc :user_id)) 
                                            providers)}}}
      {:error 100
       :content {:error {:code 100
                         :errormessage "Not found. No providers returned."}}})
    {:error 100
     :content {:error {:code 100
                       :errormessage "Not found. User not found."}}}))

(defmethod format-providers :application/xml
  [_ {username :username} providers]
  (if username
    (if (first providers)
      {:error 0
       :content-type "application/xml"
       :content (->> (map (fn [provider]
                            (xml/element :Provider {}
                                         (xml/element :AgencyID {} (:agencyid provider))
                                         (xml/element :ID {} (:id provider))))
                          providers)
                     (xml/element :Providers {:UserName username})
                     (xml/emit-str))}
      {:error 100
       :content-type "application/xml"
       :content (xml/emit-str (sdmx-error 100 "Not found. No providers returned."))})
    {:error 100
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 100 "Not found. User not found."))}))

(defn retrieve-all-providers [db content-type]
  (format-providers content-type 
                    {:username "ALL"}
                    (get-providers db)))


(defn retrieve-providers-by-user [db content-type user]
  (if-let [user (get-user db user)]
    (format-providers content-type 
                      user
                      (get-providers-by-user db user))
    (format-providers content-type 
                      nil
                      [nil])))

;; User modification functions

(defmulti format-modify-provider
  (fn [content-type _]
    (-> content-type
        (clojure.string/replace #";" "_")
        (clojure.string/replace #"=" "-")
        keyword)))

(defmethod format-modify-provider :application/json
  [_ result]
  (if (= 1 result)
    {:error 0
     :content {:success {:successmessage "Provider successfully updated."}}}
    {:error 1006
     :content {:error {:code 1006
                       :errormessage "Provider update failed."}}}))

(defmethod format-modify-provider :application/xml
  [_ result]
  (if (= 1 result)
    {:error 0
     :content-type "application/xml"
     :content (->> (xml/element :Success {}
                                (xml/element :SuccessMessage {} 
                                             "Provider successfully updated."))
                   (xml/emit-str))}
    {:error 1006
     :content-type "application/xml"
     :content (xml/emit-str (sdmx-error 1006 "Provider update failed."))}))

(defn create-provider [db content-type user provider]
  (if-let [user (get-user db user)] 
    (if-not (get-provider db (merge user provider))
      (format-modify-provider content-type
                              (insert-provider db (merge user 
                                                         provider)))
      (format-modify-provider content-type 0))
    (format-modify-provider content-type 0)))

(defn remove-provider [db content-type user provider]
  (if-let [user (get-user db user)]
    (format-modify-provider content-type 
                            (delete-provider db (merge user
                                                       provider)))
    (format-modify-provider content-type 0)))

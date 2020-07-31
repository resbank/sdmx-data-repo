(ns spartadata.handler.userapi
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [spartadata.model.user.admin :as admin]
            [spartadata.model.user.log :as data-log]
            [spartadata.model.user.provider :as provider]
            [spartadata.model.user.role :as role]
            [spartadata.sdmx.errors :refer [sdmx-error sdmx-response]]
            [clojure.data.xml :as xml]))



;; Users


(defn retrieve-all-users 
  [{connection :conn {content-type "accept"} :headers}]
  (let [user-message (try (admin/retrieve-all-users {:datasource connection} 
                                               content-type) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/retrieve-all-users."
                                              :user nil
                                              :user-fields nil})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; get all users."))}))]
    (sdmx-response user-message)))

(defn retrieve-self 
  [{connection :conn {content-type "accept"} :headers user :identity}]
  (let [user-message (try (admin/retrieve-self content-type
                                               user) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/retrieve-self."
                                              :user user
                                              :user-fields nil})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; get profile."))}))]
    (sdmx-response user-message)))

(defn update-self
  [{connection :conn {content-type "accept"} :headers user :identity {user-fields :query} :parameters}]
  (let [user-message (try (admin/update-self {:datasource connection} 
                                             content-type
                                             user
                                             user-fields) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/update-self"
                                              :user (:username user)
                                              :user-fields user-fields})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; update self."))}))]
    (sdmx-response user-message)))

(defn retrieve-self-profile 
  [{connection :conn {content-type "accept"} :headers user :identity}]
  (let [user-message (try (admin/retrieve-self-profile content-type
                                                       user) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/retrieve-self-profile."
                                              :user (:username user)
                                              :user-fields nil})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; get self profile."))}))]
    (sdmx-response user-message)))

(defn create-user 
  [{connection :conn {content-type "accept"} :headers {user :path user-fields :query} :parameters}]
  (let [user-message (try (admin/create-user {:datasource connection} 
                                             content-type
                                             user
                                             user-fields) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/create-user."
                                              :user user
                                              :user-fields user-fields})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; create user."))}))]
    (sdmx-response user-message)))

(defn retrieve-user 
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (let [user-message (try (admin/retrieve-user {:datasource connection} 
                                               content-type
                                               user) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/retrieve-user."
                                              :user user
                                              :user-fields nil})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; get user."))}))]
    (sdmx-response user-message)))

(defn update-user 
  [{connection :conn {content-type "accept"} :headers {user :path user-fields :query} :parameters}]
  (let [user-message (try (admin/update-user {:datasource connection} 
                                             content-type
                                             user
                                             user-fields) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/update-user."
                                              :user user
                                              :user-fields user-fields})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; update user."))}))]
    (sdmx-response user-message)))

(defn remove-user 
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (let [user-message (try (admin/remove-user {:datasource connection} 
                                             content-type
                                             user) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/remove-user."
                                              :user user
                                              :user-fields nil})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; remove user."))}))]
    (sdmx-response user-message)))

(defn retrieve-profile 
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (let [user-message (try (admin/retrieve-profile {:datasource connection} 
                                                  content-type
                                                  user) 
                          (catch Exception error 
                            (log/error error {:error "Error in admin/retrieve-user."
                                              :user user
                                              :user-fields nil})
                            {:error 500
                             :content-type "application/xml"
                             :content (xml/emit-str (sdmx-error 500 "Failed to complete action; get profile."))}))]
    (sdmx-response user-message)))



;; Providers


(defn retrieve-all-providers 
  [{connection :conn {content-type "accept"} :headers}]
  (let [provider-message (try (provider/retrieve-all-providers {:datasource connection} 
                                                               content-type) 
                              (catch Exception error 
                                (log/error error {:error "Error in provider/retrieve-all-providers."
                                                  :user nil
                                                  :provider nil})
                                {:error 500
                                 :content-type "application/xml"
                                 :content (xml/emit-str (sdmx-error 500 "Failed to complete action; get all providers."))}))]
    (sdmx-response provider-message)))

(defn retrieve-providers-by-user
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (let [provider-message (try (provider/retrieve-providers-by-user {:datasource connection} 
                                                                   content-type
                                                                   user) 
                              (catch Exception error 
                                (log/error error {:error "Error in provider/retrieve-providers-by-user."
                                                  :user user
                                                  :provider nil})
                                {:error 500
                                 :content-type "application/xml"
                                 :content (xml/emit-str (sdmx-error 500 "Failed to complete action; get providers by user."))}))]
    (sdmx-response provider-message)))

(defn create-provider 
  [{connection :conn {content-type "accept"} :headers {path-params :path} :parameters}]
  (let [provider-message (try (provider/create-provider {:datasource connection} 
                                                        content-type
                                                        (select-keys path-params [:username])
                                                        (->> (string/split (:strict-provider-ref path-params) #",")
                                                             (zipmap [:agencyid :id]))) 
                              (catch Exception error 
                                (log/error error {:error "Error in provider/create-provider."
                                                  :user (:username path-params)
                                                  :provider (:strict-provider-ref path-params)})
                                {:error 500
                                 :content-type "application/xml"
                                 :content (xml/emit-str (sdmx-error 500 "Failed to complete action; create provider."))}))]
    (sdmx-response provider-message)))

(defn remove-provider 
  [{connection :conn {content-type "accept"} :headers {path-params :path} :parameters}]
  (let [provider-message (try (provider/remove-provider {:datasource connection} 
                                                        content-type
                                                        (select-keys path-params [:username])
                                                        (->> (string/split (:strict-provider-ref path-params) #",")
                                                             (zipmap [:agencyid :id]))) 
                              (catch Exception error 
                                (log/error error {:error "Error in provider/remove-provider."
                                                  :user (:username path-params)
                                                  :provider (:strict-provider-ref path-params)})
                                {:error 500
                                 :content-type "application/xml"
                                 :content (xml/emit-str (sdmx-error 500 "Failed to complete action; remove provider."))}))]
    (sdmx-response provider-message)))



;; Roles



;; Log

(ns spartadata.handler.userapi
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [spartadata.model.user.admin :as admin]
;            [spartadata.model.user.log :as data-log]
            [spartadata.model.user.provider :as provider]
;            [spartadata.model.user.role :as role]
            [spartadata.utilities :refer [format-error format-response]]))



;; Users


(defn retrieve-all-users 
  [{connection :conn {content-type "accept"} :headers}]
  (-> (try (admin/retrieve-all-users {:datasource connection} content-type) 
           (catch Exception error 
             (log/error error {:error "Error in admin/retrieve-all-users."
                               :user nil
                               :user-fields nil})
             (format-error content-type 500  "Failed to complete action; get all users.")))
      format-response))

(defn retrieve-self 
  [{{content-type "accept"} :headers user :identity}]
  (-> (try (admin/retrieve-self content-type
                                user) 
           (catch Exception error 
             (log/error error {:error "Error in admin/retrieve-self."
                               :user user
                               :user-fields nil})
             (format-error content-type 500  "Failed to complete action; get profile.")))
      format-response))

(defn update-self
  [{connection :conn {content-type "accept"} :headers user :identity {user-fields :query} :parameters}]
  (-> (try (admin/update-self {:datasource connection} 
                              content-type
                              user
                              user-fields) 
           (catch Exception error 
             (log/error error {:error "Error in admin/update-self"
                               :user (:username user)
                               :user-fields user-fields})
             (format-error content-type 500  "Failed to complete action; update self.")))
      format-response))

(defn retrieve-self-profile 
  [{{content-type "accept"} :headers user :identity}]
  (-> (try (admin/retrieve-self-profile content-type
                                        user) 
           (catch Exception error 
             (log/error error {:error "Error in admin/retrieve-self-profile."
                               :user (:username user)
                               :user-fields nil})
             (format-error content-type 500  "Failed to complete action; get self profile.")))
      format-response))

(defn create-user 
  [{connection :conn {content-type "accept"} :headers {user :path user-fields :query} :parameters}]
  (-> (try (admin/create-user {:datasource connection} 
                              content-type
                              user
                              user-fields) 
           (catch Exception error 
             (log/error error {:error "Error in admin/create-user."
                               :user user
                               :user-fields user-fields})
             (format-error content-type 500 "Failed to complete action; create user.")))
      format-response))

(defn retrieve-user 
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (-> (try (admin/retrieve-user {:datasource connection} 
                                content-type
                                user) 
           (catch Exception error 
             (log/error error {:error "Error in admin/retrieve-user."
                               :user user
                               :user-fields nil})
             (format-error content-type 500  "Failed to complete action; get user.")))
      format-response))

(defn update-user 
  [{connection :conn {content-type "accept"} :headers {user :path user-fields :query} :parameters}]
  (-> (try (admin/update-user {:datasource connection} 
                              content-type
                              user
                              user-fields) 
           (catch Exception error 
             (log/error error {:error "Error in admin/update-user."
                               :user user
                               :user-fields user-fields})
             (format-error content-type 500  "Failed to complete action; update user.")))
      format-response))

(defn remove-user 
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (-> (try (admin/remove-user {:datasource connection} 
                              content-type
                              user) 
           (catch Exception error 
             (log/error error {:error "Error in admin/remove-user."
                               :user user
                               :user-fields nil})
             (format-error content-type 500  "Failed to complete action; remove user.")))
      format-response))

(defn retrieve-profile 
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (-> (try (admin/retrieve-profile {:datasource connection} 
                                   content-type
                                   user) 
           (catch Exception error 
             (log/error error {:error "Error in admin/retrieve-user."
                               :user user
                               :user-fields nil})
             (format-error content-type 500"Failed to complete action; get profile.")))
      format-response))



;; Providers


(defn retrieve-all-providers 
  [{connection :conn {content-type "accept"} :headers}]
  (-> (try (provider/retrieve-all-providers {:datasource connection} 
                                            content-type) 
           (catch Exception error 
             (log/error error {:error "Error in provider/retrieve-all-providers."
                               :user nil
                               :provider nil})
             (format-error content-type 500  "Failed to complete action; get all providers.")))
      format-response))

(defn retrieve-providers-by-user
  [{connection :conn {content-type "accept"} :headers {user :path} :parameters}]
  (-> (try (provider/retrieve-providers-by-user {:datasource connection} 
                                                content-type
                                                user) 
           (catch Exception error 
             (log/error error {:error "Error in provider/retrieve-providers-by-user."
                               :user user
                               :provider nil})
             (format-error content-type 500  "Failed to complete action; get providers by user.")))
      format-response))

(defn create-provider 
  [{connection :conn {content-type "accept"} :headers {path-params :path} :parameters}]
  (-> (try (provider/create-provider {:datasource connection} 
                                     content-type
                                     (select-keys path-params [:username])
                                     (->> (string/split (:strict-provider-ref path-params) #",")
                                          (zipmap [:agencyid :id]))) 
           (catch Exception error 
             (log/error error {:error "Error in provider/create-provider."
                               :user (:username path-params)
                               :provider (:strict-provider-ref path-params)})
             (format-error content-type 500  "Failed to complete action; create provider..")))
      format-response))

(defn remove-provider 
  [{connection :conn {content-type "accept"} :headers {path-params :path} :parameters}]
  (-> (try (provider/remove-provider {:datasource connection} 
                                     content-type
                                     (select-keys path-params [:username])
                                     (->> (string/split (:strict-provider-ref path-params) #",")
                                          (zipmap [:agencyid :id]))) 
           (catch Exception error 
             (log/error error {:error "Error in provider/remove-provider."
                               :user (:username path-params)
                               :provider (:strict-provider-ref path-params)})
             (format-error content-type 500  "Failed to complete action; remove provider.")))
      format-response))



;; Roles



;; Log

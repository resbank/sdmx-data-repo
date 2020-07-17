(ns spartadata.auth
  (:require [buddy.auth.accessrules :refer [success error]]
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
            [clojure.data.xml :as xml]
            [clojure.set :as sets]
            [clojure.zip :as zip]
            [hugsql.core :as sql]
            [reitit.middleware :as middleware]
            [spartadata.sdmx.errors :refer [sdmx-error]]
            [spartadata.sdmx.util :as util]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")



;; Define function that is responsible for authenticating requests.
;; In this case it receives a map with username and password and it
;; should return a value that can be considered a "user" instance
;; and should be a logical true.


(defn authfn [db req {:keys [username password]}]
  (when-let [user (get-user db {:username (clojure.string/upper-case username)})]
    (when (= password (:password user))
      user)))



;; Create an instance of auth backend without explicit handler for
;; unauthorized request (that leaves the responsibility to default
;; backend implementation).


(defn backend [connection-pool]
  (http-basic-backend {:realm "SDMX Rest API"
                       :authfn (partial authfn {:datasource connection-pool})}))



;; Reitit middleware


(def authentication
  (middleware/map->Middleware
    {:name ::authentication
     :description "Authentication middleware."
     :wrap wrap-authentication}))



;; Access rules handlers

(defn on-error [request value]
  {:status 401
   :headers {"content-type" "application/xml"}
   :body (xml/emit-str (sdmx-error 110 (str "Not authorised. " value)))})

(defn should-be-authenticated [request]
  (if (:identity request)
    true
    (error "Only authenticated users allowed")))

(declare get-dataset)
(declare get-dataset-roles)
(declare get-datasets)
(declare get-providers)

(xml/alias-uri 'messg "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
(xml/alias-uri 'struc "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")

(defn data-query [connection-pool request]
  (if-let [user (:identity request)]
    (let [dataflows (util/filter-dataflows (get-in request [:parameters :path :flow-ref])
                                           (get-datasets {:datasource connection-pool}))
          providers (util/filter-data-providers (get-in request [:parameters :path :provider-ref])
                                                (get-providers {:datasource connection-pool} user))
          agreements (util/filter-provision-agreements dataflows
                                                       providers)]
      (if (seq agreements)
        (success (str "Provision agreement(s) available: " agreements)) 
        (error "Must be registered with a valid provision agreement.")))
    (error "Must be logged in to perform this action.")))

(defn should-be-owner [connection-pool request]
  (if-let [user (:identity request)]
    (let [dataset (get-dataset {:datasource connection-pool} 
                               (sets/rename-keys (get-in request [:parameters :path])
                                                 {:agency-id :agencyid :resource-id :id}))
          roles (get-dataset-roles {:datasource connection-pool}
                                   (merge user dataset))]
      (if (seq (sets/intersection #{"owner" "admin"} 
                                  (into #{} (map :role roles))))
        (success "Authorisation confirmed.") 
        (error "Must be dataset owner or administrator to perform this action.")))
    (error "Must be logged in to perform this action.")))

(defn should-be-admin [connection-pool request]
  (if-let [user (:identity request)]
    (let [dataset (get-dataset {:datasource connection-pool} 
                               (sets/rename-keys (get-in request [:parameters :path])
                                                 {:agency-id :agencyid :resource-id :id}))
          roles (get-dataset-roles {:datasource connection-pool}
                                   (merge user dataset))]
      (if (seq (sets/intersection #{"owner" "admin"} 
                                  (into #{} (map :role roles))))
        (success "Authorisation confirmed.") 
        (error "Must be dataset administrator to perform this action.")))
    (error "Must be logged in to perform this action.")))

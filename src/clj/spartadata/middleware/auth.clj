(ns spartadata.middleware.auth
  (:require [buddy.auth.accessrules :refer [success error wrap-access-rules]]
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.middleware :refer [wrap-authentication]]
            [clojure.data.xml :as xml]
            [clojure.set :as sets]
            [clojure.string :as string]
            [hugsql.core :as sql]
            [reitit.middleware :as middleware]
            [spartadata.sdmx.errors :refer [sdmx-error]]))



;; Import needed SQL functions


(sql/def-db-fns "sql/query.sql")

(declare get-dataset)
(declare get-dataset-roles)
(declare get-user)

;; Define function that is responsible for authenticating requests.
;; In this case it receives a map with username and password and it
;; should return a value that can be considered a "user" instance
;; and should be a logical true.


(defn authfn [db _ {:keys [username password]}]
  (when-let [user (get-user db {:username (string/upper-case username)})]
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



(defn on-error [_ value]
  {:status 401
   :headers {"Content-Type" "application/xml"}
   :body (xml/emit-str (sdmx-error 110 (str "Not authorised. " value)))})

(defn should-be-authenticated [request]
  (if (:identity request)
    (success "Authorisation confirmed.") 
    (error "Must authenticate successfully to perform this action.")))

(defn should-have-provision-agreement [request]
  (if (:identity request)
    (if (seq (:sdmx-data-query request))
      (success "Authorisation confirmed.") 
      (error "Must have a valid provision agreement to perform this action."))
    (error "Must authenticate successfully to perform this action.")))

(defn should-be-owner [request]
  (if-let [user (:identity request)]
    (let [dataset (get-dataset {:datasource (:conn request)} 
                               (zipmap [:agencyid :id :version] (clojure.string/split (:strict-flow-ref (get-in request [:parameters :path])) #",")))
          roles (get-dataset-roles {:datasource (:conn request)}
                                   (merge user dataset))]
      (if (contains? (into #{} (map :role roles)) "owner")
        (success "Authorisation confirmed.") 
        (error "Must be data set owner or administrator to perform this action.")))
    (error "Must authenticate successfully to perform this action.")))

(defn should-be-admin [request]
  (if-let [user (:identity request)]
    (if (:is_admin user)
        (success "Authorisation confirmed.") 
        (error "Must be administrator to perform this action."))
    (error "Must authenticate successfully to perform this action.")))


;; Reitit middleware

(def options {:rules [{:pattern #"^/sdmxapi/data/.*"
                       :handler should-have-provision-agreement}
                      {:pattern #"^/sdmxapi/modify/data/[^/]*"
                       :handler {:or [should-be-owner should-be-admin]} 
                       :request-method :post}
                      {:pattern #"^/sdmxapi/modify/data/[^/]*"
                       :handler should-be-admin
                       :request-method #{:delete :put}}
                      {:pattern #"^/sdmxapi/modify/data/historical/.*"
                       :handler should-be-admin}
                      {:pattern #"^/sdmxapi/modify/data/rollback/.*"
                       :handler {:or [should-be-owner should-be-admin]}}
                      {:pattern #"^/sdmxapi/release/data/.*"
                       :handler should-be-authenticated
                       :request-method :get}
                      {:pattern #"^/sdmxapi/release/data/.*"
                       :handler {:or [should-be-owner should-be-admin]}   
                       :request-method :post}]
              :on-error on-error})

(def authorisation
  (middleware/map->Middleware
    {:name ::authorisation
     :description "Authorisation middleware."
     :wrap #(wrap-access-rules % options)}))

(ns spartadata.routes
  (:require [clojure.data.xml :as xml]
            [reitit.ring :as ring]
            [reitit.coercion.spec]
            [reitit.swagger :as swagger]
            [reitit.swagger-ui :as swagger-ui]
            [reitit.ring.coercion :as coercion]
            [reitit.dev.pretty :as pretty]
            [reitit.ring.middleware.muuntaja :as muuntaja]
            [reitit.ring.middleware.exception :as exception]
            [reitit.ring.middleware.multipart :as multipart]
            [reitit.ring.middleware.parameters :as parameters]
;            [reitit.ring.middleware.dev :as dev]
            [spartadata.handler.sdmxapi :as sdmxapi]
            [spartadata.handler.userapi :as userapi]
            [spartadata.middleware.auth :as auth]
            [spartadata.middleware.conn :refer [conn]]
            [spartadata.middleware.data-query-resolution :refer [resolve-data-query]]
            [spartadata.middleware.log :as log]
            [spartadata.sdmx.errors :refer [sdmx-error]]
            [spartadata.sdmx.spec]
            [muuntaja.core :as m]))



;; Ring handler


(defn router [connection-pool context]
  (ring/ring-handler
    (ring/router
      [["/sdmxapi" {:swagger {:id ::sdmxapi}}

        ["/swagger.json"
         {:get {:no-doc true
                :swagger {:info {:title "Sparta Data SDMX Rest API"}
                          :securityDefinitions {:basicAuth {:type "basic"
                                                            :name "Authorization"
                                                            :in "header"}}}
                :handler (swagger/create-swagger-handler)}}]

        ["" {:swagger {:security [{:basicAuth []}]}
             :middleware [[auth/authentication (auth/backend connection-pool)]]}

          ["/data" {:middleware [resolve-data-query 
                                 [auth/authorisation context]]}

           ["/{flow-ref}"
            {:get {:tags ["Retrieve"]
                   :summary "Get data set"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-1
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmxapi/data}}]

           ["/{flow-ref}/{key}"
            {:get {:tags ["Retrieve"]
                   :summary "Get data set (specify series key)"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-2
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmxapi/data}}]

           ["/{flow-ref}/{key}/{provider-ref}"
            {:get {:tags ["Retrieve"]
                   :summary "Get data set (specify provider)"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-3
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmxapi/data}}]]

        ["" {:middleware [[auth/authorisation context]]}

         ["/modify/data" {:middleware [[log/data-change context]]}

          ["/{strict-flow-ref}"
           {:put {:tags ["Modify"]
                  :summary "Create data set"
                  :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                               :query :spartadata.sdmx.spec/data-create-query-params
                               :multipart {:file multipart/temp-file-part}}
                  :handler sdmxapi/data-create}
            :post {:tags ["Modify"]
                   :summary "Update data set"
                   :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                                :query :spartadata.sdmx.spec/data-upload-query-params
                                :multipart {:file multipart/temp-file-part}}
                   :handler sdmxapi/data-upload}
            :delete {:tags ["Modify"]
                     :summary "Delete data set"
                     :parameters {:path :spartadata.sdmx.spec/data-delete-path-params}
                     :handler sdmxapi/data-delete}}]

          ["/{strict-flow-ref}/historical"
           {:post {:tags ["Modify"]
                   :summary "Update data set (historical update)"
                   :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                                :query :spartadata.sdmx.spec/data-upload-hist-query-params
                                :multipart {:file multipart/temp-file-part}}
                   :handler sdmxapi/data-upload-hist}}]

          ["/{strict-flow-ref}/rollback"
           {:post {:tags ["Modify"]
                   :summary "Update data set (rollback release)"
                   :parameters {:path :spartadata.sdmx.spec/data-rollback-path-params}
                   :handler sdmxapi/data-rollback}}]]

         ["/release/data" 

          ["/{strict-flow-ref}"
           {:get {:tags ["Release"]
                  :summary "Get data set releases"
                  :parameters {:path :spartadata.sdmx.spec/data-releases-path-params
                               :query :spartadata.sdmx.spec/data-releases-query-params}
                  :handler sdmxapi/data-releases}
            :post {:middleware [[log/data-change context]]
                   :tags ["Release"]
                   :summary "Update data set releases (add release)"
                   :parameters {:path :spartadata.sdmx.spec/data-release-path-params
                                :query :spartadata.sdmx.spec/data-release-query-params}
                   :handler sdmxapi/data-release}}]]]]]
       
       ["/userapi" {:swagger {:id ::userapi}}

        ["/swagger.json"
         {:get {:no-doc true
                :swagger {:info {:title "Sparta Data User Rest API"}
                          :securityDefinitions {:basicAuth {:type "basic"
                                                            :name "Authorization"
                                                            :in "header"}}}
                :handler (swagger/create-swagger-handler)}}]

        ["" {:swagger {:security [{:basicAuth []}]}
             :middleware [[auth/authentication (auth/backend connection-pool)]
                          [auth/authorisation context]]}

         ["/log"
          
          ["/data"
           {:get {:tags ["Log"]
                  :parameters {:query :spartadata.sdmx.spec/log-query-params}
                  :summary "Get data log"
                  :handler userapi/retrieve-data-log}}]
          
          ["/users"
           {:get {:tags ["Log"]
                  :parameters {:query :spartadata.sdmx.spec/log-query-params}
                  :summary "Get users log"
                  :handler userapi/retrieve-user-log}}]]

         ["/users"
          {:get {:tags ["User"]
                 :summary "Get all users"
                 :handler userapi/retrieve-all-users}}]

         ["/self"
          {:get {:tags ["User"]
                 :summary "Get own user"
                 :handler userapi/retrieve-self}
           :post {:tags ["User"]
                  :parameters {:query :spartadata.sdmx.spec/user-self-query-params}
                  :summary "Update own user"
                  :handler userapi/update-self}}]

         ["/self/profile"
          {:get {:tags ["User"]
                 :summary "Get own user profile"
                 :handler userapi/retrieve-self-profile}}]

         ["/user" {:middleware [[log/user-change context]]}

          ["/{username}"
           {:put {:tags ["User"]
                  :summary "Create user"
                  :parameters {:path :spartadata.sdmx.spec/user-path-params
                               :query :spartadata.sdmx.spec/user-create-query-params}
                  :handler userapi/create-user}
            :get {:tags ["User"]
                  :summary "Get user"
                  :parameters {:path :spartadata.sdmx.spec/user-path-params}
                  :handler userapi/retrieve-user}
            :post {:tags ["User"]
                   :summary "Update user"
                   :parameters {:path :spartadata.sdmx.spec/user-path-params
                               :query :spartadata.sdmx.spec/user-update-query-params}
                   :handler userapi/update-user}
            :delete {:tags ["User"]
                     :summary "Delete user"
                     :parameters {:path :spartadata.sdmx.spec/user-path-params}
                     :handler userapi/remove-user}}]

          ["/{username}/profile" 
           {:get {:tags ["User"]
                  :summary "Get user profile"
                  :parameters {:path :spartadata.sdmx.spec/user-path-params}
                  :handler userapi/retrieve-profile}}]

          ["/{username}/providers"
           {:get {:tags ["Provider"]
                  :summary "Get data user's providers"
                  :parameters {:path :spartadata.sdmx.spec/user-path-params}
                  :handler userapi/retrieve-providers}}]

          ["/{username}/provider/{strict-provider-ref}"
           {:put {:tags ["Provider"]
                  :summary "Add data provider"
                  :parameters {:path :spartadata.sdmx.spec/provider-path-params}
                  :handler userapi/create-provider}
            :delete {:tags ["Provider"]
                     :summary "Delete data provider"
                     :parameters {:path :spartadata.sdmx.spec/provider-path-params}
                     :handler userapi/remove-provider}}]

          ["/{username}/roles"
           {:get {:tags ["Role"]
                  :summary "Get user's data set roles"
                  :parameters {:path :spartadata.sdmx.spec/user-path-params}
                  :handler userapi/retrieve-roles}}]

          ["/{username}/role/{role}/dataflow/{strict-flow-ref}"
           {:put {:tags ["Role"]
                  :summary "Add data set role"
                  :parameters {:path :spartadata.sdmx.spec/role-path-params}
                  :handler userapi/create-role} 
            :delete {:tags ["Role"]
                     :summary "Delete data set role"
                     :parameters {:path :spartadata.sdmx.spec/role-path-params}
                     :handler userapi/remove-role}}]]]]]

      (cond-> {:exception pretty/exception
               :data {:coercion reitit.coercion.spec/coercion
                      :muuntaja m/instance
                      :middleware [swagger/swagger-feature
                                   parameters/parameters-middleware
                                   muuntaja/format-negotiate-middleware
                                   muuntaja/format-response-middleware
                                   exception/exception-middleware
                                   muuntaja/format-request-middleware
                                   coercion/coerce-response-middleware
                                   coercion/coerce-request-middleware
                                   multipart/multipart-middleware
                                   [conn connection-pool]]}}
        context (assoc :path context)))

    (ring/routes

      (swagger-ui/create-swagger-ui-handler
        {:path (str context "/api-docs")
         :url (str context "/sdmxapi/swagger.json")
         :config {:validatorUrl nil
                  :operationsSorter "alpha"}})

      (ring/create-resource-handler
        {:path (or context "/")})

      (ring/create-default-handler
        {:not-found (constantly {:status 404 
                                 :headers {"Content-Type" "application/xml"} 
                                 :body (xml/emit-str (sdmx-error 100 "No results found."))})
         :method-not-allowed (constantly {:status 405 
                                          :headers {"Content-Type" "application/xml"} 
                                          :body (xml/emit-str (sdmx-error 1004 "Method not allowed."))})
         :not-acceptable (constantly {:status 406 
                                      :headers {"Content-Type" "application/xml"} 
                                      :body (xml/emit-str (sdmx-error 1006 "Not Acceptable."))})}))))

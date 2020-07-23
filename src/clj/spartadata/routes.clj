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
            [reitit.ring.middleware.dev :as dev]
            [spartadata.handler.sdmxapi :as sdmx]
            [spartadata.middleware.auth :as auth]
            [spartadata.middleware.conn :refer [conn]]
            [spartadata.middleware.data-query-resolution :refer [resolve-data-query]]
            [spartadata.sdmx.errors :refer [sdmx-error]]
            [spartadata.sdmx.spec :refer :all]
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
                   :handler sdmx/data}}]

           ["/{flow-ref}/{key}"
            {:get {:tags ["Retrieve"]
                   :summary "Get data set (specify series key)"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-2
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmx/data}}]

           ["/{flow-ref}/{key}/{provider-ref}"
            {:get {:tags ["Retrieve"]
                   :summary "Get data set (specify provider)"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-3
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmx/data}}]]

        ["" {:middleware [[auth/authorisation context]]}

         ["/modify/data" 

          ["/{strict-flow-ref}"
           {:put {:tags ["Modify"]
                  :summary "Create data set"
                  :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                               :query :spartadata.sdmx.spec/data-create-query-params
                               :multipart {:file multipart/temp-file-part}}
                  :handler sdmx/data-create}
            :post {:tags ["Modify"]
                   :summary "Update data set"
                   :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                                :query :spartadata.sdmx.spec/data-upload-query-params
                                :multipart {:file multipart/temp-file-part}}
                   :handler sdmx/data-upload}
            :delete {:tags ["Modify"]
                     :summary "Delete data set"
                     :parameters {:path :spartadata.sdmx.spec/data-delete-path-params}
                     :handler sdmx/data-delete}}]

          ["/{strict-flow-ref}/historical"
           {:post {:tags ["Modify"]
                   :summary "Update data set (historical update)"
                   :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                                :query :spartadata.sdmx.spec/data-upload-hist-query-params
                                :multipart {:file multipart/temp-file-part}}
                   :handler sdmx/data-upload-hist}}]

          ["/{strict-flow-ref}/rollback"
           {:post {:tags ["Modify"]
                   :summary "Update data set (rollback release)"
                   :parameters {:path :spartadata.sdmx.spec/data-rollback-path-params}
                   :handler sdmx/data-rollback}}]]

         ["/release/data" 

          ["/{strict-flow-ref}"
          {:get {:tags ["Release"]
                 :summary "Get data set releases"
                 :parameters {:path :spartadata.sdmx.spec/data-releases-path-params
                              :query :spartadata.sdmx.spec/data-releases-query-params}
                 :handler sdmx/data-releases}
           :post {:tags ["Release"]
                  :summary "Update data set releases (add release)"
                  :parameters {:path :spartadata.sdmx.spec/data-release-path-params
                               :query :spartadata.sdmx.spec/data-release-query-params}
                  :handler sdmx/data-release}}]
          
          ["/{strict-flow-ref}/historical"
           {:post {:tags ["Release"]
                  :summary "Update data set releases (add historical release)"
                  :parameters {:path :spartadata.sdmx.spec/data-release-path-params
                               :query :spartadata.sdmx.spec/data-release-hist-query-params}
                  :handler sdmx/data-release-hist}}]]]]]
       
       ["/userapi" {:swagger {:id ::userapi}}

        ["/swagger.json"
         {:get {:no-doc true
                :swagger {:info {:title "Sparta Data User Rest API"}
                          :securityDefinitions {:basicAuth {:type "basic"
                                                            :name "Authorization"
                                                            :in "header"}}}
                :handler (swagger/create-swagger-handler)}}]

        ["/user" {:swagger {:security [{:basicAuth []}]}
                  :middleware [[auth/authentication (auth/backend connection-pool)]]}

          ["/{user}" 
           {:put {:tags ["User"]
                  :summary "Create user"
                  :handler (constantly {:status 200
                                        :body {}})}
            :get {:tags ["User"]
                  :summary "Get user"
                  :handler (constantly {:status 200
                                        :body {}})}
            :post {:tags ["User"]
                   :summary "Update user"
                   :handler (constantly {:status 200
                                         :body {}})}
            :delete {:tags ["User"]
                     :summary "Delete user"
                     :handler (constantly {:status 200
                                           :body {}})}}]
            
           ["/{user}/provider"
            {:put {:tags ["Provider"]
                   :summary "Add data provider"
                   :handler (constantly {:status 200
                                         :body {}})}
             :get {:tags ["Provider"]
                   :summary "Get data provider"
                   :handler (constantly {:status 200
                                         :body {}})}
             :delete {:tags ["Provider"]
                      :summary "Delete data provider"
                      :handler (constantly {:status 200
                                            :body {}})}}]
           
           ["/{user}/role"
            {:put {:tags ["Role"]
                   :summary "Add data set role"
                   :handler (constantly {:status 200
                                         :body {}})} 
             :get {:tags ["Role"]
                   :summary "Get data set role"
                   :handler (constantly {:status 200
                                         :body {}})}
             :delete {:tags ["Role"]
                      :summary "Delete data set role"
                      :handler (constantly {:status 200
                                            :body {}})}}]]]]

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

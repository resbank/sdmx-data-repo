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
      [["/sdmxapi" 

        ["/swagger.json"
         {:get {:no-doc true
                :swagger {:info {:title "SDMX Rest API"}
                          :securityDefinitions {:basicAuth {:type "basic"
                                                            :name "Authorization"
                                                            :in "header"}}}
                :handler (swagger/create-swagger-handler)}}]

        ["" {:swagger {:security [{:basicAuth []}]}
             :middleware [[auth/authentication (auth/backend connection-pool)]]}

          ["/data" {:middleware [resolve-data-query 
                                 auth/authorisation]}

           ["/{flow-ref}"
            {:get {:tags ["Data"]
                   :summary "Data set"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-1
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmx/data}}]

           ["/{flow-ref}/{key}"
            {:get {:tags ["Data"]
                   :summary "Data set"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-2
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmx/data}}]

           ["/{flow-ref}/{key}/{provider-ref}"
            {:get {:tags ["Data"]
                   :summary "Data set"
                   :parameters {:path :spartadata.sdmx.spec/data-path-params-3
                                :query :spartadata.sdmx.spec/data-query-params}
                   :handler sdmx/data}}]]

        ["" {:middleware [auth/authorisation]}

         ["/modify/data" 

          ["/{strict-flow-ref}"
           {:post {:tags ["Data"]
                   :summary "Data set"
                   :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                                :query :spartadata.sdmx.spec/data-upload-query-params
                                :multipart {:file multipart/temp-file-part}}
                   :handler sdmx/data-upload}
            :delete {:tags ["Data"]
                     :summary "Data set"
                     :parameters {:path :spartadata.sdmx.spec/data-delete-path-params}
                     :handler sdmx/data-delete}}]

          ["/historical/{strict-flow-ref}"
           {:post {:tags ["Data"]
                   :summary "Historical data set"
                   :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                                :query :spartadata.sdmx.spec/data-upload-hist-query-params
                                :multipart {:file multipart/temp-file-part}}
                   :handler sdmx/data-upload-hist}}]

          ["/rollback/{strict-flow-ref}"
           {:post {:tags ["Data"]
                   :summary "Rollback data set"
                   :parameters {:path :spartadata.sdmx.spec/data-rollback-path-params}
                   :handler sdmx/data-rollback}}]]

         ["/release/data/{strict-flow-ref}" 
          {:get {:tags ["Data"]
                 :summary "Data set releases"
                 :parameters {:path :spartadata.sdmx.spec/data-releases-path-params
                              :query :spartadata.sdmx.spec/data-releases-query-params}
                 :handler sdmx/data-releases}
           :post {:tags ["Data"]
                  :summary "Data set release"
                  :parameters {:path :spartadata.sdmx.spec/data-release-path-params
                               :query :spartadata.sdmx.spec/data-release-query-params}
                  :handler sdmx/data-release}}]]]

        ["/metadata/{flow-ref}/{key}/{provider-ref}"
         {:get {:tags ["Metadata"]
                :summary "Metadata set"
                :parameters {:path :spartadata.sdmx.spec/data-path-params-3
                             :query :spartadata.sdmx.spec/data-query-params}
                :handler sdmx/metadata}}]]]

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
        {:path (str context "/sdmxapi")
         :url (str context "/sdmxapi/swagger.json")
         :config {:validatorUrl nil
                  :operationsSorter "alpha"}})

      (ring/create-resource-handler
        {:path (or context "/")})

      (ring/create-default-handler
        {:not-found (constantly {:status 404, :body (xml/emit-str (sdmx-error 100 "No results found."))})
         :method-not-allowed (constantly {:status 405, :body (xml/emit-str (sdmx-error 1004 "Method not allowed."))})
         :not-acceptable (constantly {:status 406, :body (xml/emit-str (sdmx-error 1006 "Not Acceptable."))})}))))

(ns spartadata.routes
  (:require [reitit.ring :as ring]
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
            [reitit.ring.spec :as spec]
            [spec-tools.spell :as spell]
            [spartadata.handler.sdmxapi :as sdmx]
            [spartadata.sdmx.spec :refer :all]
            [muuntaja.core :as m]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]))



;; Ring handler


(defn handler [connection-pool]
  (ring/ring-handler
    (ring/router
      [["/SpartaData"

        ["/swagger.json"
         {:get {:no-doc true
                :swagger {:info {:title "SDMX Rest API"}}
                :handler (swagger/create-swagger-handler)}}]

        ["/ws/public/sdmxapi/rest" 

         ["/data/{flow-ref}"
          {:get {:tags ["Data and Metadata Queries"]
                 :summary "SDMX data query"
                 :parameters {:path :spartadata.sdmx.spec/data-path-params-1
                              :query :spartadata.sdmx.spec/data-query-params}
                 :handler (partial sdmx/data connection-pool)}}]

         ["/data/{flow-ref}/{key}"
          {:get {:tags ["Data and Metadata Queries"]
                 :summary "SDMX data query"
                 :parameters {:path :spartadata.sdmx.spec/data-path-params-2
                              :query :spartadata.sdmx.spec/data-query-params}
                 :handler (partial sdmx/data connection-pool)}}]

         ["/data/{flow-ref}/{key}/{provider-ref}"
          {:get {:tags ["Data and Metadata Queries"]
                 :summary "SDMX data query"
                 :parameters {:path :spartadata.sdmx.spec/data-path-params-3
                              :query :spartadata.sdmx.spec/data-query-params}
                 :handler (partial sdmx/data connection-pool)}}]

         ["/data/upload/{agency-id}/{resource-id}/{version}"
          {:post {:tags ["Data and Metadata Queries"]
                  :summary "SDMX data post"
                  :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                               :query :spartadata.sdmx.spec/data-upload-query-params
                               :multipart {:file multipart/temp-file-part}}
                  :handler (partial sdmx/data-upload connection-pool)}}]

         ["/data/upload/historical/{agency-id}/{resource-id}/{version}"
          {:post {:tags ["Data and Metadata Queries"]
                  :summary "SDMX historical data post"
                  :parameters {:path :spartadata.sdmx.spec/data-upload-path-params
                               :query :spartadata.sdmx.spec/data-upload-hist-query-params
                               :multipart {:file multipart/temp-file-part}}
                  :handler (partial sdmx/data-upload-hist connection-pool)}}]

         ["/data/rollback/{agency-id}/{resource-id}/{version}"
          {:post {:tags ["Data and Metadata Queries"]
                  :summary "SDMX data rollback post"
                  :parameters {:path :spartadata.sdmx.spec/data-rollback-path-params}
                  :handler (partial sdmx/data-rollback connection-pool)}}]

         ["/metadata/{flow-ref}/{key}/{provider-ref}"
          {:get {:tags ["Data and Metadata Queries"]
                 :summary "SDMX metadata query"
                 :parameters {:path :spartadata.sdmx.spec/data-path-params-3
                              :query :spartadata.sdmx.spec/data-query-params}
                 :handler sdmx/metadata}}]

         ["/datastructure/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:tags ["Structural Metadata Queries"]
                 :summary "SDMX Data Structure Definition query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/metadatastructure/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:tags ["Structural Metadata Queries"]
                 :summary "SDMX Metadata Structure Definition query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/categoryscheme/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-3}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Category Scheme query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-3
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/conceptscheme/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-1}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Concept Scheme query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-1
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/codelist/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-3}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Codelist query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-3
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/hierarchicalcodelist/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-2}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Hierarchical Codelist query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-2
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/organisationscheme/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-3}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Organisation Scheme query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-3
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/agencyscheme/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-1}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Agency Scheme query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-1
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/dataproviderscheme/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-3}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Data Provider Scheme query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-3
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/dataconsumerscheme/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-3}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Data Consumer Scheme query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-3
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/organisationunitscheme/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-3}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Organisation Unit Scheme query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-3
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/dataflow/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Data Flow query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/metadataflow/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Metadata Flow Definition query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/reportingtaxonomy/{nested-agency-id}/{nested-resource-id}/{nested-version}/{item-id-3}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Reporting Taxonomy query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params-3
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/provisionagreement/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Provision Agreement query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/structureset/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Structure Set query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/process/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Process query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/categorisation/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Categorisation query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/contentconstraint/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Content Constraint query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/actualconstraint/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Actual Constraint query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/allowedconstraint/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Allowed Constraint query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/attachmentconstraint/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Attachment Constraint query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/structure/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Structural Metadata Queries"]}
                 :summary "SDMX Structure query"
                 :parameters {:path :spartadata.sdmx.spec/struct-path-params
                              :query :spartadata.sdmx.spec/struct-query-params}
                 :handler sdmx/structure}}]

         ["/schema/{context}/{nested-agency-id}/{nested-resource-id}/{nested-version}"
          {:get {:swagger {:tags ["Schema Queries"]}
                 :summary "SDMX Data Structure Definition query"
                 :parameters {:path :spartadata.sdmx.spec/schema-path-params
                              :query :spartadata.sdmx.spec/schema-query-params}
                 :handler sdmx/schema}}]

         ["/other/{flow-ref}/{key}/{provider-ref}/{component-id}"
          {:get {:swagger {:tags ["Other Queries"]}
                 :summary "SDMX metadata query"
                 :parameters {:path :spartadata.sdmx.spec/other-path-params
                              :query :spartadata.sdmx.spec/other-query-params}
                 :handler sdmx/other}}]]]]

      {:exception pretty/exception
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
                           multipart/multipart-middleware]}})
    (ring/routes
      (swagger-ui/create-swagger-ui-handler
        {:path "/SpartaData"
         :url "/SpartaData/swagger.json"
         :config {:validatorUrl nil
                  :operationsSorter "alpha"}})
      (ring/create-default-handler
        {:not-found (constantly {:status 404, :body "kosh"})
         :method-not-allowed (constantly {:status 405, :body "kosh"})
         :not-acceptable (constantly {:status 406, :body "kosh"})}))))

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
            [spartadata.handlers.sdmx :as sdmx]
            [muuntaja.core :as m]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]))

;; Data and metadata spec

(def rest-types (edn/read-string (slurp "resources/sdmx/rest-types.edn")))
(s/def ::flow-ref (s/and string? #(re-matches (re-pattern (:flow-ref-type rest-types)) %)))
(s/def ::key (s/and string? #(re-matches (re-pattern (:key-type rest-types)) %)))
(s/def ::provider-ref (s/and string? #(re-matches (re-pattern (:provider-ref-type rest-types)) %)))

(s/def ::data-path-params (s/keys :req-un [::flow-ref] :opt-un [::key ::provider-ref]))

(s/def ::startPeriod (s/and string? #(re-matches #"\d{4}-\d{2}-\d{2}" %)))
(s/def ::endPeriod (s/and string? #(re-matches #"\d{4}-\d{2}-\d{2}" %)))
(s/def ::updatedAfter (s/and string? #(re-matches #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" %)))
(s/def ::firstNObservations (s/and int? (partial < 0)))
(s/def ::lastNObservations (s/and int? (partial < 0)))
(s/def ::dimensionAtObservation (s/and string? #(re-matches (re-pattern (:nc-name-id-type rest-types)) %)))
(s/def ::dataDetailType string?)
(s/def ::includeHistory boolean?)

(s/def ::release (s/and string? #(re-matches #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" %)))
(s/def ::validate boolean?)

(s/def ::data-query-params 
  (s/keys :opt-un [::startPeriod ::endPeriod ::updatedAfter ::firstNObservations 
                   ::lastNObservations ::dimensionAtObservation ::dataDetailType
                   ::includeHistory ::release ::validate]))

(s/def ::agency-id (s/and string? #(re-matches (re-pattern (:nc-name-id-type rest-types)) %)))
(s/def ::resource-id (s/and string? #(re-matches (re-pattern (:id-type rest-types)) %)))
(s/def ::version (s/and string? #(re-matches (re-pattern (:multiple-versions-type rest-types)) %)))

(s/def ::data-upload-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

(s/def ::nextRelease (s/and string? #(re-matches #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" %)))
(s/def ::releaseDescription string?)
(s/def ::insertAsRelease boolean?)

(s/def ::data-upload-query-params (s/keys :opt-un [::insertAsRelease ::releaseDescription]))

(s/def ::data-upload-hist-query-params (s/keys :req-un [::nextRelease] :opt-un [::releaseDescription]))

(s/def ::data-rollback-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

;; Structural metadata spec

(s/def ::struct-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

(s/def ::item-id-1 (s/and string? #(re-matches (re-pattern (:nested-nc-name-id-type rest-types)) %)))
(s/def ::item-id-2 (s/and string? #(re-matches (re-pattern (:id-type rest-types)) %)))
(s/def ::item-id-3 (s/and string? #(re-matches (re-pattern (:nested-id-type rest-types)) %)))

(s/def ::struct-path-params-1 (s/keys :req-un [::agency-id ::resource-id ::version] :opt-un [::item-id-1]))
(s/def ::struct-path-params-2 (s/keys :req-un [::agency-id ::resource-id ::version] :opt-un [::item-id-2]))
(s/def ::struct-path-params-3 (s/keys :req-un [::agency-id ::resource-id ::version] :opt-un [::item-id-3]))

(s/def ::detail string?)
(s/def ::references string?)

(s/def ::struct-query-params (s/keys :opt-un [::detail ::references]))

;; Schema spec

(s/def ::context (s/and string? #(re-matches (re-pattern #"datastructure|metadatastructure|dataflow|metadataflow|provisonagreement") %)))

(s/def ::schema-path-params (s/keys :req-un [::context ::agency-id ::resource-id ::version]))

(s/def ::explicitMeasure boolean?)

(s/def ::schema-query-params (s/keys :opt-un [::dimensionAtObservation ::explicitMeasure]))

;; Other spec

(s/def ::component-id (s/and string? #(re-matches (re-pattern (:id-type rest-types)) %)))

(s/def ::other-path-params (s/keys :req-un [::agency-id ::resource-id ::version ::component-id]))

(s/def ::mode string?)

(s/def ::other-query-params (s/keys :opt-un [::startPeriod ::endPeriod ::updatedAfter ::references ::mode]))



;; Reitit router


(defn router [connection-pool]
  (ring/ring-handler
    (ring/router
      [["/swagger.json"
        {:get {:no-doc true
               :swagger {:info {:title "SDMX Rest API"}}
               :handler (swagger/create-swagger-handler)}}]

       ["/sdmxapi/rest" 

        ["/data/{flow-ref}"
         {:get {:tags ["Data and Metadata Queries"]
                :summary "SDMX data query"
                :parameters {:path ::data-path-params
                             :query ::data-query-params}
                :handler (partial sdmx/data connection-pool)}}]

        ["/data/{flow-ref}/{key}"
         {:get {:tags ["Data and Metadata Queries"]
                :summary "SDMX data query"
                :parameters {:path ::data-path-params
                             :query ::data-query-params}
                :handler (partial sdmx/data connection-pool)}}]

        ["/data/{flow-ref}/{key}/{provider-ref}"
         {:get {:tags ["Data and Metadata Queries"]
                :summary "SDMX data query"
                :parameters {:path ::data-path-params
                             :query ::data-query-params}
                :handler (partial sdmx/data connection-pool)}}]

        ["/data/upload/{agency-id}/{resource-id}/{version}"
         {:post {:tags ["Data and Metadata Queries"]
                 :summary "SDMX data post"
                 :parameters {:path ::data-upload-path-params
                              :query ::data-upload-query-params}
                 :handler (partial sdmx/data-upload connection-pool)}}]

        ["/data/upload/historical/{agency-id}/{resource-id}/{version}"
         {:post {:tags ["Data and Metadata Queries"]
                 :summary "SDMX historical data post"
                 :parameters {:path ::data-upload-path-params
                              :query ::data-upload-hist-query-params}
                 :handler (partial sdmx/data-upload-hist connection-pool)}}]

        ["/data/rollback/{agency-id}/{resource-id}/{version}"
         {:post {:tags ["Data and Metadata Queries"]
                 :summary "SDMX data rollback post"
                 :parameters {:path ::data-rollback-path-params}
                 :handler (partial sdmx/data-rollback connection-pool)}}]

        ["/metadata/{flow-ref}/{key}/{provider-ref}"
         {:get {:tags ["Data and Metadata Queries"]
                :summary "SDMX metadata query"
                :parameters {:path ::data-path-params
                             :query ::data-query-params}
                :handler sdmx/metadata}}]

        ["/datastructure/{agency-id}/{resource-id}/{version}"
         {:get {:tags ["Structural Metadata Queries"]
                :summary "SDMX Data Structure Definition query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/metadatastructure/{agency-id}/{resource-id}/{version}"
         {:get {:tags ["Structural Metadata Queries"]
                :summary "SDMX Metadata Structure Definition query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/categoryscheme/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Category Scheme query"
                :parameters {:path ::struct-path-params-3
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/conceptscheme/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Concept Scheme query"
                :parameters {:path ::struct-path-params-1
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/codelist/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Codelist query"
                :parameters {:path ::struct-path-params-3
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/hierarchicalcodelist/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Hierarchical Codelist query"
                :parameters {:path ::struct-path-params-2
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/organisationscheme/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Organisation Scheme query"
                :parameters {:path ::struct-path-params-3
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/agencyscheme/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Agency Scheme query"
                :parameters {:path ::struct-path-params-1
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/dataproviderscheme/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Data Provider Scheme query"
                :parameters {:path ::struct-path-params-3
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/dataconsumerscheme/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Data Consumer Scheme query"
                :parameters {:path ::struct-path-params-3
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/organisationunitscheme/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Organisation Unit Scheme query"
                :parameters {:path ::struct-path-params-3
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/dataflow/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Data Flow query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/metadataflow/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Metadata Flow Definition query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/reportingtaxonomy/{agency-id}/{resource-id}/{version}/{item-id}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Reporting Taxonomy query"
                :parameters {:path ::struct-path-params-3
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/provisionagreement/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Provision Agreement query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/structureset/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Structure Set query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/process/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Process query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/categorisation/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Categorisation query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/contentconstraint/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Content Constraint query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/actualconstraint/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Actual Constraint query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/allowedconstraint/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Allowed Constraint query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/attachmentconstraint/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Attachment Constraint query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/structure/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Structural Metadata Queries"]}
                :summary "SDMX Structure query"
                :parameters {:path ::struct-path-params
                             :query ::struct-query-params}
                :handler sdmx/structure}}]

        ["/schema/{context}/{agency-id}/{resource-id}/{version}"
         {:get {:swagger {:tags ["Schema Queries"]}
                :summary "SDMX Data Structure Definition query"
                :parameters {:path ::schema-path-params
                             :query ::schema-query-params}
                :handler sdmx/schema}}]

        ["/other/{flow-ref}/{key}/{provider-ref}/{component-id}"
         {:get {:swagger {:tags ["Other Queries"]}
                :summary "SDMX metadata query"
                :parameters {:path ::other-path-params
                             :query ::other-query-params}
                :handler sdmx/other}}]]]

      {;;:reitit.middleware/transform dev/print-request-diffs ;; pretty diffs
       ;;:validate spec/validate ;; enable spec validation for route data
       ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
       :exception pretty/exception
       :data {:coercion reitit.coercion.spec/coercion
              :muuntaja m/instance
              :middleware [;; swagger feature
                           swagger/swagger-feature
                           ;; query-params & form-params
                           parameters/parameters-middleware
                           ;; content-negotiation
                           muuntaja/format-negotiate-middleware
                           ;; encoding response body
                           muuntaja/format-response-middleware
                           ;; exception handling
                           exception/exception-middleware
                           ;; decoding request body
                           muuntaja/format-request-middleware
                           ;; coercing response bodys
                           coercion/coerce-response-middleware
                           ;; coercing request parameters
                           coercion/coerce-request-middleware]}})
    (ring/routes
      (swagger-ui/create-swagger-ui-handler
        {:path "/"
         :config {:validatorUrl nil
                  :operationsSorter "alpha"}})
      (ring/create-default-handler
        {:not-found (constantly {:status 404, :body "kosh"})
         :method-not-allowed (constantly {:status 405, :body "kosh"})
         :not-acceptable (constantly {:status 406, :body "kosh"})}))))

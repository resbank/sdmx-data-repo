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
            [clojure.spec.alpha :as s]
            [clojure.java.io :as io]))

(def id-type #"[\w@\$\-]+(\+[\w@\$\-]+)*")
(def nested-nc-name-id-type #"[A-Za-z][\w\-]*(\.[A-Za-z][\w\-]*)*(\+[A-Za-z][\w\-]*(\.[A-Za-z][\w\-]*)*)*")
(def multiple-versions-type #"\d+(\.\d+)*(\+\d+(\.\d+)*)*")
(def version-type (re-pattern (str "(all|latest|" multiple-versions-type ")")))

(def flow-ref-type (re-pattern (str "^(" id-type "|(" nested-nc-name-id-type "(\\," id-type ")(\\," version-type ")?))$")))
(def key-type (re-pattern (str "^(" id-type ")?" "(\\.(" id-type ")?)*$")))
(def provider-ref-type (re-pattern (str "^(" nested-nc-name-id-type "\\,)?" id-type "$")))

(s/def ::flow-ref (s/and string? #(re-matches flow-ref-type %)))
(s/def ::key (s/and string? #(re-matches key-type %)))
(s/def ::provider-ref (s/and string? #(re-matches provider-ref-type %)))

(s/def ::data-request (s/keys :req-un [::flow-ref ::key ::provider-ref]))

(defn router [connection-pool]
  (ring/ring-handler
    (ring/router
      [["/swagger.json"
        {:get {:no-doc true
               :swagger {:info {:title "SDMX Rest API"}}
               :handler (swagger/create-swagger-handler)}}]

       ["/sdmxapi/rest"
        {:swagger {:tags ["Data queries"]}}

        ["/data/{flow-ref}/{key}/{provider-ref}"
         {:get {:summary "SDMX data query"
                :parameters {:path ::data-request}
                :handler (partial sdmx/data connection-pool)}}]

        ["/metadata/{flow-ref}/{key}/{provider-ref}"
         {:get {:summary "SDMX metadata query"
                :parameters {:path ::data-request}
                :handler (fn [request]
                           {:status 200
                            :body (apply str request)})}}]]]
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
      (ring/create-default-handler))))

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
            [muuntaja.core :as m]
            [clojure.spec.alpha :as s]
            [clojure.java.io :as io]))

(defn delimit-with [delim regex]
  (re-pattern (str regex "(" delim regex ")*")))

(def id-type (delimit-with "\\+" #"[\w@\$\-]+"))
(def nested-nc-name-id-type (->> (delimit-with "\\." #"[A-Za-z][\w\-]*") (delimit-with "\\+")))
(def all-type #"all")
(def latest-type #"latest")
(def multiple-versions-type (->> (delimit-with "\\." #"\d+") (delimit-with "\\+")))
(def version-type (re-pattern (str "(" all-type "|" latest-type "|" multiple-versions-type ")")))
(def flow-ref-type (re-pattern (str "^(" id-type "|(" nested-nc-name-id-type "(\\," id-type ")(\\," version-type ")?))$")))
(def key-type (re-pattern (str "^(" id-type ")?" "(\\.(" id-type ")?)*$")))
(def provider-ref-type (re-pattern (str "^(" nested-nc-name-id-type "\\,)?" id-type "$")))

(s/def ::flow-ref (s/and string? #(re-matches flow-ref-type %)))
(s/def ::key (s/and string? #(re-matches key-type %)))
(s/def ::provider-ref (s/and string? #(re-matches provider-ref-type %)))

(s/def ::data-request (s/keys :req-un [::flow-ref ::key ::provider-ref]))

(defn router []
  (ring/ring-handler
    (ring/router
      [["/swagger.json"
        {:get {:no-doc true
               :swagger {:info {:title "SDMX Rest API"}}
               :handler (swagger/create-swagger-handler)}}]

       ["/sdmx/restapi"
        {:swagger {:tags ["Data queries"]}}

        ["/data/{flow-ref}/{key}/{provider-ref}"
         {:get {:summary "SDMX data query"
                :parameters {:path ::data-request}
                :handler (fn [request]
                           (let [params (get-in request [:parameters :path])]
                           {:status 200
                            :body (str {:flow-ref (:flow-ref params)
                                        :key (map #(clojure.string/split % #"\+") (clojure.string/split (:key params) #"\." -1))
                                        :provider-ref (:provider-ref params)})}))}}]

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

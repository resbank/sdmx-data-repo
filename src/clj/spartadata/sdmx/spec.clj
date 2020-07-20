(ns spartadata.sdmx.spec
  (:require [clojure.spec.alpha :as s]))

;; Types conforming *loosely* to SDMX Rest types

(def all-type #"all")
(def latest-type #"latest")
(def id-type #"[\w@\$\-]+(\.[\w@\$\-]+)*")
(def nc-name-id-type #"[A-Za-z][\w\-]*(\.[A-Za-z][\w\-]*)*")
(def version-type #"\d+(\.\d+)*")
(def nested-id-type (re-pattern (str id-type "(\\+" id-type ")*")))
(def nested-nc-name-id-type (re-pattern (str nc-name-id-type "(\\+" nc-name-id-type ")*")))
(def nested-version-type (re-pattern (str version-type "(\\+" version-type ")*")))
(def multiple-version-type (re-pattern (str latest-type "|" all-type "|" nested-version-type)))
(def key-type #"([\w@\$\-]+(\+[\w@\$\-]+)*)?(\.([\w@\$\-]+(\+[\w@\$\-]+)*)?)*")
(def provider-ref-type #"([A-Za-z][\w\-]*(\.[A-Za-z][\w\-]*)*(\+[A-Za-z][\w\-]*(\.[A-Za-z][\w\-]*)*)*\,)?[\w@\$\-]+(\+[\w@\$\-]+)*")
(def flow-ref-type #"([\w@\$\-]+(\+[\w@\$\-]+)*|([A-Za-z][\w\-]*(\.[A-Za-z][\w\-]*)*(\+[A-Za-z][\w\-]*(\.[A-Za-z][\w\-]*)*)*(\,[\w@\$\-]+(\+[\w@\$\-]+)*)(\,(all|latest|\d+(\.\d+)*(\+\d+(\.\d+)*)*))?))")

;; Specs for API end points

;; Data and metadata spec

(s/def ::flow-ref (s/and string? #(re-matches flow-ref-type %)))
(s/def ::key (s/and string? #(re-matches key-type %)))
(s/def ::provider-ref (s/and string? #(re-matches provider-ref-type  %)))

(s/def ::data-path-params-1 (s/keys :req-un [::flow-ref]))
(s/def ::data-path-params-2 (s/keys :req-un [::flow-ref ::key]))
(s/def ::data-path-params-3 (s/keys :req-un [::flow-ref ::key ::provider-ref]))

(s/def ::startPeriod (s/and string? #(re-matches #"^\d{4}-\d{2}-\d{2}$" %)))
(s/def ::endPeriod (s/and string? #(re-matches #"^\d{4}-\d{2}-\d{2}$" %)))
(s/def ::updatedAfter (s/and string? #(re-matches #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" %)))
(s/def ::firstNObservations (s/and int? (partial < 0)))
(s/def ::lastNObservations (s/and int? (partial < 0)))
(s/def ::dimensionAtObservation (s/and string? #(re-matches nc-name-id-type  %)))
(s/def ::dataDetailType string?)
(s/def ::includeHistory boolean?)

(s/def ::format string?)
(s/def ::releaseDescription string?)
(s/def ::validate boolean?)

(s/def ::data-query-params 
  (s/keys :opt-un [::startPeriod ::endPeriod ::updatedAfter ::firstNObservations 
                   ::lastNObservations ::dimensionAtObservation ::dataDetailType
                   ::includeHistory ::releaseDescription ::validate ::format]))

(s/def ::agency-id (s/and string? #(re-matches nc-name-id-type  %)))
(s/def ::resource-id (s/and string? #(re-matches id-type  %)))
(s/def ::version (s/and string? #(re-matches version-type  %)))

(s/def ::data-upload-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

(s/def ::releaseDateTime (s/and string? #(re-matches #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" %)))

(s/def ::data-upload-query-params (s/keys :opt-un [::releaseDescription ::validate]))

(s/def ::data-upload-hist-query-params (s/keys :req-un [::releaseDateTime ::releaseDescription ] :opt-un [::validate]))

(s/def ::data-rollback-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

(s/def ::newest boolean?)
(s/def ::oldest boolean?)
(s/def ::afterDateTime (s/and string? #(re-matches #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" %)))
(s/def ::beforeDateTime (s/and string? #(re-matches #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}" %)))
(s/def ::includesText string?)

(s/def ::data-releases-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

(s/def ::data-releases-query-params (s/keys :opt-un [::newest ::oldest ::afterDateTime ::beforeDateTime ::includesText ::releaseDescription]))

(s/def ::data-release-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

(s/def ::data-release-query-params (s/keys :req-un [::releaseDescription]))

(s/def ::data-delete-path-params (s/keys :req-un [::agency-id ::resource-id ::version]))

;; Structural metadata spec

(s/def ::nested-agency-id (s/and string? #(re-matches nested-nc-name-id-type  %)))
(s/def ::nested-resource-id (s/and string? #(re-matches nested-id-type  %)))
(s/def ::nested-version (s/and string? #(re-matches multiple-version-type  %)))

(s/def ::struct-path-params (s/keys :req-un [::nested-agency-id ::nested-resource-id ::nested-version]))

(s/def ::item-id-1 (s/and string? #(re-matches nested-nc-name-id-type  %)))
(s/def ::item-id-2 (s/and string? #(re-matches id-type %)))
(s/def ::item-id-3 (s/and string? #(re-matches nested-id-type  %)))

(s/def ::struct-path-params-1 (s/keys :req-un [::nested-agency-id ::nested-resource-id ::nested-version] :opt-un [::item-id-1]))
(s/def ::struct-path-params-2 (s/keys :req-un [::nested-agency-id ::nested-resource-id ::nested-version] :opt-un [::item-id-2]))
(s/def ::struct-path-params-3 (s/keys :req-un [::nested-agency-id ::nested-resource-id ::nested-version] :opt-un [::item-id-3]))

(s/def ::detail string?)
(s/def ::references string?)

(s/def ::struct-query-params (s/keys :opt-un [::detail ::references]))

;; Schema spec

(s/def ::context (s/and string? #(re-matches (re-pattern #"^datastructure|metadatastructure|dataflow|metadataflow|provisonagreement$") %)))

(s/def ::schema-path-params (s/keys :req-un [::context ::nested-agency-id ::nested-resource-id ::nested-version]))

(s/def ::explicitMeasure boolean?)

(s/def ::schema-query-params (s/keys :opt-un [::dimensionAtObservation ::explicitMeasure]))

;; Other spec

(s/def ::component-id (s/and string? #(re-matches id-type  %)))

(s/def ::other-path-params (s/keys :req-un [::nested-agency-id ::nested-resource-id ::nested-version ::component-id]))

(s/def ::mode string?)

(s/def ::other-query-params (s/keys :opt-un [::startPeriod ::endPeriod ::updatedAfter ::references ::mode]))

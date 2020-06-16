(ns spartadata.sdmx.validate
  (:require [clojure.java.io :as io]
            [environ.core :refer [env]]
            [spartadata.sdmx.errors :refer [sdmx-error]])
  (:import [javax.xml XMLConstants transform.Source transform.stream.StreamSource validation.SchemaFactory validation.Schema validation.Validator]
           [org.xml sax.SAXException]))



;; XML validation function


(defn validate-xml [schema xml]
  (try
    (-> XMLConstants/W3C_XML_SCHEMA_NS_URI
        (SchemaFactory/newInstance)
        (.newSchema schema)
        (.newValidator)
        (.validate xml))
    (catch SAXException error (sdmx-error 1000 (str "Validation failed. " (.getMessage error))))))



;; Define multimethod


(defmulti validate-data (fn [dataflow data-message opts] (:format opts)))



;; Define methods for the varios message formats


(defmethod validate-data "application/vnd.sdmx.compact+xml;version=2.0"
  [{agencyid :agency-id id :resource-id version :version} data-message opts]
  (let [dataflow (str (:sdmx-registry env) "/sdmxapi/rest/schema/dataflow/" agencyid  "/" id "/" version "?format=sdmx-2.0")]
    (with-open [message (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXMessage.xsd"))
                structure (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXStructure.xsd"))
                generic-data (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXGenericData.xsd"))
                utility-data (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXUtilityData.xsd"))
                compact-data (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXCompactData.xsd"))
                cross-sectional-data (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXCrossSectionalData.xsd"))
                query (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXQuery.xsd"))
                common (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXCommon.xsd"))
                registry (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXRegistry.xsd"))
                generic-metadata (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXGenericMetadata.xsd"))
                metadata-report (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/SDMXMetadataReport.xsd"))
                xml (clojure.java.io/input-stream  (clojure.java.io/resource "schema/v2_0/xml.xsd"))
                data-message (if (string? data-message) (clojure.java.io/reader (char-array data-message)) (clojure.java.io/input-stream data-message))]
      (validate-xml (into-array [(StreamSource. xml)
                                 (StreamSource. common)
                                 (StreamSource. structure)
                                 (StreamSource. query)
                                 (StreamSource. registry)
                                 (StreamSource. generic-metadata)
                                 (StreamSource. metadata-report)
                                 (StreamSource. cross-sectional-data)
                                 (StreamSource. compact-data)
                                 (StreamSource. generic-data)
                                 (StreamSource. utility-data)
                                 (StreamSource. message)
                                 (StreamSource. dataflow)])
                    (StreamSource. data-message)))))

(defmethod validate-data :default
  [{agencyid :agency-id id :resource-id version :version} data-message opts]
  (sdmx-error 1001 "Validation not supported. Unable to validate unsupported format."))

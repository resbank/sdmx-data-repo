(ns spartadata.sdmx.validate
  (:require [clojure.java.io :as io]
            [environ.core :refer [env]])
  (:import [javax.xml XMLConstants transform.Source transform.stream.StreamSource validation.SchemaFactory validation.Schema validation.Validator]
           [org.xml sax.SAXException]))

(defn validate-data [{agencyid :agency-id id :resource-id version :version} data-message]
  (with-open [message-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXMessage.xsd"))
              structure-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXStructure.xsd"))
              generic-data-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXGenericData.xsd"))
              utility-data-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXUtilityData.xsd"))
              compact-data-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXCompactData.xsd"))
              cross-sectional-data-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXCrossSectionalData.xsd"))
              query-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXQuery.xsd"))
              common-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXCommon.xsd"))
              registry-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXRegistry.xsd"))
              generic-metadata-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXGenericMetadata.xsd"))
              metadata-report-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/SDMXMetadataReport.xsd"))
              xml-schema (clojure.java.io/input-stream  (clojure.java.io/resource "sdmx/v2_0/xml.xsd"))
              data-message (if (string? data-message) 
                             (clojure.java.io/reader (char-array data-message))
                             (clojure.java.io/input-stream data-message))]
    (-> XMLConstants/W3C_XML_SCHEMA_NS_URI
        (SchemaFactory/newInstance)
        (.newSchema (into-array [(StreamSource. xml-schema)
                                 (StreamSource. common-schema)
                                 (StreamSource. structure-schema)
                                 (StreamSource. query-schema)
                                 (StreamSource. registry-schema)
                                 (StreamSource. generic-metadata-schema)
                                 (StreamSource. metadata-report-schema)
                                 (StreamSource. cross-sectional-data-schema)
                                 (StreamSource. compact-data-schema)
                                 (StreamSource. generic-data-schema)
                                 (StreamSource. utility-data-schema)
                                 (StreamSource. message-schema)
                                 (StreamSource. (str (:sdmx-registry env) "/sdmxapi/rest/schema/dataflow/" 
                                                     agencyid  "/" id "/" version "?format=sdmx-2.0"))]))
        (.newValidator)
        (.validate (StreamSource. data-message)))))

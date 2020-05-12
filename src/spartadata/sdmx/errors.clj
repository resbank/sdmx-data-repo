(ns spartadata.sdmx.errors
  (:require [clojure.data.xml :as xml]))


(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")
(xml/alias-uri 'commn "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")
(xml/alias-uri 'xsi "http://www.w3.org/2001/XMLSchema-instance")

(defn sdmx-error [code text]
  (xml/element ::messg/Error {::xsi/schemaLocation 
                              (str "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message "
                                   "https://registry.sdmx.org/schemas/v2_1/SDMXMessage.xsd")}
               (xml/element ::messg/ErrorMessage {:code code}
                            (xml/element ::commn/Text {} text))))

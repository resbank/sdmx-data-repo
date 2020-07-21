(ns spartadata.sdmx.errors
  (:require [clojure.data.xml :as xml]))


;; Error codes


;; SDMX error                                     HTTP status code
;;
;; 100 No results found                           404 Not found
;; 110 Unauthorized 	                            401 Unauthorized
;; 130 Response too large due to client request   413 Request entity too large
;; 140 Syntax error                               400 Bad syntax
;; 150 Semantic error                             403 Forbidden
;; 500 Internal Server error                      500 Internal server error
;; 501 Not implemented                            501 Not implemented
;; 503 Service unavailable                        503 Service unavailable
;; 510 Response size exceeds service limit        413 Request entity too large
;; 1000+ Custom errors                            500 Internal server error
;;
;; 1000 Validation failed
;; 1001 Validation not supported
;; 1002 Format not supported
;; 1003 Upload failed
;; 1004 Method not allowed
;; 1005 Not Acceptable



;; Error message generating function


(xml/alias-uri 'messg "http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message")
(xml/alias-uri 'commn "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")
(xml/alias-uri 'xsi "http://www.w3.org/2001/XMLSchema-instance")

(defn sdmx-error [code text]
  (xml/element ::messg/Error {::xsi/schemaLocation 
                              (str "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message "
                                   "https://registry.sdmx.org/schemas/v2_1/SDMXMessage.xsd")}
               (xml/element ::messg/ErrorMessage {:code code}
                            (xml/element ::commn/Text {} text))))

(defn sdmx-response [data-message]
  (cond-> (cond 
            (= 100 (:error data-message)) {:status 404}
            (= 110 (:error data-message)) {:status 401}
            (= 130 (:error data-message)) {:status 413}
            (= 140 (:error data-message)) {:status 400}
            (= 150 (:error data-message)) {:status 403}
            (= 500 (:error data-message)) {:status 500}
            (= 501 (:error data-message)) {:status 501}
            (= 503 (:error data-message)) {:status 503}
            (= 510 (:error data-message)) {:status 413}
            (= 1000 (:error data-message)) {:status 500}
            (< 1000 (:error data-message)) {:status 500}
            :else {:status 200})
    (:content-type data-message) (assoc :headers {"Content-Type" (:content-type data-message)})
    :always (assoc :body (:content data-message))))

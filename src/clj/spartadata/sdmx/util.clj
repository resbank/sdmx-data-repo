;; Utility functions used throughout the application


(ns spartadata.sdmx.util)



;; Calculate the string edit distance, used to match the release description in the REST API


(defn levenshtein-distance 
  "Calculate the edit distance between two strings (Wagner–Fischer algorithm)"
  [s t]
  ;; Iterate over the input string (columns)
  (loop [index (map inc (range (count s)))
         col (range (inc (count t)))]
    (if-not index
      (last col)
      (recur (next index) 
             ;; Iterate over the target string (rows)
             (loop [[b & t] t
                    [head & remain] col
                    a (nth s (dec (first index)))
                    next-col [(first index)]]
               (if (empty? remain)
                 next-col
                 (recur t 
                        remain 
                        a 
                        (conj next-col
                              (min (+ (last next-col) 1) 
                                   (+ (first remain) 1)
                                   (+ head (if (= a b) 0 1)))))))))))

(ns spartadata.userapi-test
  (:require [clj-http.client :as client]
            [clojure.test :refer :all]
            [java-time]
            [jsonista.core :as json]
            [user]))



;; Globals


(def mapper
  (json/object-mapper
    {:decode-key-fn keyword}))



;; Helper functions

(defn log-lookup [action admin usr afterDateTime]
  (-> "http://localhost:3030/userapi/log/users" 
      (client/get {:basic-auth ["p512788" "password"]
                   :query-params {:afterDateTime afterDateTime}})
      :body
      (json/read-value mapper)
      :log
      (as-> $ 
        (filter (fn [log-entry] 
                  (= (select-keys log-entry
                                  [:action
                                   :admin
                                   :usr])
                     {:action action
                      :admin admin
                      :usr usr})) $))
      seq))



;; Test suite (all actions to be performed by an administrator account)


(deftest user-api

  ;; Bring up the server.
  (user/go)

  ;; Test 1
  (testing "Create user."
    (let [start (->> (java-time/sql-timestamp)
                     (java-time/local-date-time)
                     (java-time/format (java-time/formatter "YYYY-MM-dd'T'HH:mm:ss")))]
      ; Create new user.
      (client/put "http://localhost:3030/userapi/user/APITESTUSR" 
                  {:basic-auth ["p512788" "password"]
                   :query-params {:firstname "Jane"
                                  :lastname "Doe"
                                  :password "password"
                                  :email "Jane.Doe@resbank.co.za"
                                  :is_admin false}})
      ; Test that the new user has been created correctly.
      (is (= {:users
              [{:email "Jane.Doe@resbank.co.za"
                :lastname "Doe"
                :username "APITESTUSR"
                :is_admin false
                :firstname "Jane"}]}
             (-> "http://localhost:3030/userapi/user/APITESTUSR" 
                 (client/get {:basic-auth ["p512788" "password"]})
                 :body
                 (json/read-value mapper))))
      ; Test that create user action was logged correctly.
      (is (log-lookup "create" "P512788" "APITESTUSR" start))))

  ;; Test 2
  (testing "Update user."
    (let [start (->> (java-time/sql-timestamp)
                     (java-time/local-date-time)
                     (java-time/format (java-time/formatter "YYYY-MM-dd'T'HH:mm:ss")))]
      ; Update users details.
      (client/post "http://localhost:3030/userapi/user/APITESTUSR" 
                   {:basic-auth ["p512788" "password"]
                    :query-params {:firstname "John"
                                   :lastname "Doe"
                                   :password "drowssap"
                                   :email "John.Doe@resbank.co.za"
                                   :is_admin true}})
      ; Test that update was applied correctly
      (is (= {:users
              [{:username "APITESTUSR"
                :firstname "John"
                :lastname "Doe"
                :email "John.Doe@resbank.co.za"
                :is_admin true}]}
             (-> "http://localhost:3030/userapi/user/APITESTUSR" 
                 (client/get {:basic-auth ["p512788" "password"]})
                 :body
                 (json/read-value mapper))))
      ; Test that the update user action was logged correctly.
      (is (log-lookup "update" "P512788" "APITESTUSR" start))))

  ;; Test 3
  (testing "Add data provider."
    (let [start (->> (java-time/sql-timestamp)
                     (java-time/local-date-time)
                     (java-time/format (java-time/formatter "YYYY-MM-dd'T'HH:mm:ss")))]
      ; Add data provider.
      (client/put "http://localhost:3030/userapi/user/APITESTUSR/provider/SARB.ERD,INTERNAL" 
                  {:basic-auth ["p512788" "password"]})
      ; Test that data provider was added correctly.
      (is (-> "http://localhost:3030/userapi/user/APITESTUSR/providers" 
              (client/get {:basic-auth ["p512788" "password"]})
              :body
              (json/read-value mapper)
              :providers
              (as-> $ 
                (filter (fn [log-entry] 
                          (= log-entry
                             {:id "INTERNAL", 
                              :agencyid "SARB.ERD"})) $))
              seq))
      ; Test that the add data provider action was logged correctly.
      (is (log-lookup "modify_provider" "P512788" "APITESTUSR" start))))

  ;; Test 4
  (testing "Add data set role."
    (let [start (->> (java-time/sql-timestamp)
                     (java-time/local-date-time)
                     (java-time/format (java-time/formatter "YYYY-MM-dd'T'HH:mm:ss")))]
      ; Add data set role.
      (client/put "http://localhost:3030/userapi/user/APITESTUSR/role/owner/dataflow/SARB.ERD,NOWCAST,1.0" 
                  {:basic-auth ["p512788" "password"]})
      ; Test that data set role was added correctly.
      (is (-> "http://localhost:3030/userapi/user/APITESTUSR/roles" 
              (client/get {:basic-auth ["p512788" "password"]})
              :body
              (json/read-value mapper)
              :roles
              (as-> $ 
                (filter (fn [log-entry] 
                          (= log-entry
                             {:role "owner"
                              :dataset {:agencyid "SARB.ERD" 
                                        :id "NOWCAST" 
                                        :version "1.0"}})) $))
              seq))
      ; Test that the add data provider action was logged correctly.
      (is (log-lookup "modify_role" "P512788" "APITESTUSR" start))))

  ;; Test 5
  (testing "Get profile."
    ; Test that the user profile is correct.
    (is (= {:providers [{:id "INTERNAL"
                         :agencyid "SARB.ERD"}]
            :roles [{:role "owner"
                     :dataset {:id "NOWCAST"
                               :version "1.0" 
                               :agencyid "SARB.ERD"}}]
            :user {:email "John.Doe@resbank.co.za"
                   :lastname "Doe"
                   :username "APITESTUSR"
                   :is_admin true
                   :firstname "John"}}
           (-> "http://localhost:3030/userapi/user/APITESTUSR/profile" 
               (client/get {:basic-auth ["p512788" "password"]})
               :body
               (json/read-value mapper)))))

  ;; Test 6
  (testing "Remove data provider."
    (let [start (->> (java-time/sql-timestamp)
                     (java-time/local-date-time)
                     (java-time/format (java-time/formatter "YYYY-MM-dd'T'HH:mm:ss")))]
      ; Remove data provider.
      (client/delete "http://localhost:3030/userapi/user/APITESTUSR/provider/SARB.ERD,INTERNAL" 
                     {:basic-auth ["p512788" "password"]})
      ; Test that data provider was removed correctly.
      (is (= {:error {:code 100
                      :errormessage "Not found. No providers returned."}}
             (-> "http://localhost:3030/userapi/user/APITESTUSR/providers" 
                 (client/get {:basic-auth ["p512788" "password"]
                              :throw-exceptions false})
                 :body
                 (json/read-value mapper))))
      ; Test that the remove data provider action was logged correctly.
      (is (log-lookup "modify_provider" "P512788" "APITESTUSR" start))))

  ;; Test 7
  (testing "Remove data set role"
    (let [start (->> (java-time/sql-timestamp)
                     (java-time/local-date-time)
                     (java-time/format (java-time/formatter "YYYY-MM-dd'T'HH:mm:ss")))]
      ; Remove data set role.
      (client/delete "http://localhost:3030/userapi/user/APITESTUSR/role/owner/dataflow/SARB.ERD,NOWCAST,1.0" 
                     {:basic-auth ["p512788" "password"]})
      ; Test that data provider was removed correctly.
      (is (= {:error {:code 100
                      :errormessage "Not found. No roles returned for user."}}
             (-> "http://localhost:3030/userapi/user/APITESTUSR/roles" 
                 (client/get {:basic-auth ["p512788" "password"]
                              :throw-exceptions false})
                 :body
                 (json/read-value mapper))))
      ; Test that the remove data provider action was logged correctly.
      (is (log-lookup "modify_role" "P512788" "APITESTUSR" start))))
   
  ;; Test 8
  (testing "Delete user"
    (let [start (->> (java-time/sql-timestamp)
                     (java-time/local-date-time)
                     (java-time/format (java-time/formatter "YYYY-MM-dd'T'HH:mm:ss")))]
      ; Create new user.
      (client/delete "http://localhost:3030/userapi/user/APITESTUSR" 
                     {:basic-auth ["p512788" "password"]})
      ; Test that the new user has been created correctly.
      (is (= {:error {:code 100
                      :errormessage "Not found. No users returned."}}
             (-> "http://localhost:3030/userapi/user/APITESTUSR" 
                 (client/get {:basic-auth ["p512788" "password"]
                              :throw-exceptions false})
                 :body
                 (json/read-value mapper))))
      ; Test that create user action was logged correctly.
      (is (log-lookup "delete" "P512788" "APITESTUSR" start))))

  ;; Take the server down.
  (user/halt)
)

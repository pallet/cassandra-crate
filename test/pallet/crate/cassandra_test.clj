(ns pallet.crate.cassandra-test
  (:require
   [pallet.crate.cassandra :as cassandra])
  (:require
   [pallet.api :as api :refer [plan-fn]]
   [pallet.build-actions :as build-actions]
   [pallet.stevedore :as stevedore])
  (:use clojure.test
        pallet.test-utils))

(deftest cassandra-test
  []
  (let [a {:tag :n :image {:os-family :ubuntu}}]
    (is (first
         (build-actions/build-actions
          {:server a}
          (cassandra/settings {})
          (cassandra/install)
          (cassandra/configure))))))

(defn cassandra-test []
  ;; TODO - add a meaningful test
  )

(def cassandra-test-spec
  (api/server-spec
   :extends [(cassandra/server-spec {})]
   :phases {:test (plan-fn (cassandra-test))}))

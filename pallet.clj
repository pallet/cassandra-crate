;;; Pallet project configuration file

(require
 '[pallet.crate.cassandra-test :refer [cassandra-test-spec]]
 '[pallet.crates.test-nodes :refer [node-specs]])

(defproject cassandra-crate
  :provider node-specs                  ; supported pallet nodes
  :groups [(group-spec "cassandra-test"
             :count 2
             :extends [with-automated-admin-user
                       cassandra-test-spec]
             :roles #{:live-test :default :cassandra})])

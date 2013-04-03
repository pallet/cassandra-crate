(defproject com.palletops/cassandra-crate "0.8.0-alpha.1"
  :description "Pallet crate to install, configure and use cassandra."
  :url "http://palletops.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:url "git@github.com:pallet/cassandra-crate.git"}

  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.palletops/pallet "0.8.0-beta.6"]
                 [clj-yaml "0.4.0"]]
  :resource {:resource-paths ["doc-src"]
             :target-path "target/classes/pallet_crate/cassandra_crate/"
             :includes [#"doc-src/USAGE.*"]}
  :prep-tasks ["resource" "crate-doc"])

(ns pallet.crate.cassandra
  "Pallet crate to install, configure and operate Cassandra.

## See Also
- http://wiki.apache.org/cassandra/Operations
- http://www.datastax.com/docs/1.2/configuration/node_configuration"
  (:require
   [clojure.tools.logging :refer [debugf warnf]]
   [clj-yaml.core :as yaml]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [pallet.actions :refer [directory package remote-directory remote-file]]
   [pallet.api :refer [plan-fn] :as api]
   [pallet.compute :refer [service-properties]]
   [pallet.crate
    :refer [assoc-settings compute-service defplan get-settings group-name
            nodes-in-group target target-node]]
   [pallet.crate.service
    :refer [supervisor-config supervisor-config-map] :as service]
   [pallet.crate.initd]
   [pallet.crate-install :as crate-install]
   [pallet.node :refer [hardware primary-ip private-ip]]
   [pallet.script.lib :refer [config-root file]]
   [pallet.stevedore :refer [fragment]]
   [pallet.utils :refer [deep-merge]]
   [pallet.version-dispatch :refer [defmethod-version-plan
                                    defmulti-version-plan]]))

(def ^{:doc "Flag for recognising changes to configuration"}
  cassandra-config-changed-flag "cassandra-config")

;;; # Settings
(def default-jvm-opts
  "JVM options for Cassandra"
  ["-Dcom.sun.management.jmxremote.authenticate=false"
   "-Dcom.sun.management.jmxremote.port=\\${JMX_PORT}"
   "-Dcom.sun.management.jmxremote.ssl=false"
   "-Djava.net.preferIPv4Stack=true"
   "-XX:+CMSParallelRemarkEnabled"
   "-XX:+HeapDumpOnOutOfMemoryError"
   "-XX:+UseCMSInitiatingOccupancyOnly"
   "-XX:+UseConcMarkSweepGC"
   "-XX:+UseParNewGC"
   "-XX:+UseThreadPriorities"
   "-XX:CMSInitiatingOccupancyFraction=75"
   "-XX:MaxTenuringThreshold=1"
   "-XX:SurvivorRatio=8"
   "-XX:ThreadPriorityPolicy=42"
   "-Xms\\${HEAP_NEWSIZE}"
   "-Xmn\\${YOUNG_GEN_SIZE}"
   "-Xmx\\${MAX_HEAP_SIZE}"
   "-Xss\\${STACK_SIZE}"
   "-javaagent:\\${CASSANDRA_HOME}/lib/jamm-0.2.5.jar"
   "-ea"])

;;; http://www.datastax.com/docs/0.8/configuration/node_configuration
;;; TODO - make these version specific
(defn default-settings
  "Default cassandra configuration options"
  []
  {:config-dir (fragment (file (config-root) "cassandra"))
   :owner "cassandra"
   :user "cassandra"
   :group "cassandra"
   :service-name "cassandra"
   :supervisor :initd
   :version "1.1"
   :group-data-centers nil
   :max-seeds 1
   :server
   {:authenticator "org.apache.cassandra.auth.AllowAllAuthenticator"
    :authority "org.apache.cassandra.auth.AllowAllAuthority"
    :cluster_name "pallet-cluster"
    :column_index_size_in_kb 64
    :commitlog_directory "/mnt/cassandra/commitlog"
    :commitlog_sync "periodic"
    :commitlog_sync_period_in_ms 20000
    :commitlog_total_space_in_mb 4096
    :compaction_preheat_key_cache true
    :concurrent_reads 16
    :concurrent_writes 32
    :data_file_directories ["/mnt/cassandra/data"]
    :dynamic_snitch_badness_threshold 0.0
    :dynamic_snitch_reset_interval_in_ms 600000
    :dynamic_snitch_update_interval_in_ms 100
    :encryption_options {:internode_encryption "none"
                         :keystore "conf/.keystore"
                         :keystore_password "cassandra"
                         :truststore "conf/.truststore"
                         :truststore_password "cassandra"}
    :flush_largest_memtables_at 0.75
    :hinted_handoff_enabled true
    :in_memory_compaction_limit_in_mb 64
    :incremental_backups false
    :index_interval 128
    :initial_token nil
    :max_hint_window_in_ms 3600000
    :memtable_flush_queue_size 4
    :memtable_total_space_in_mb 4096
    :multithreaded_compaction false
    :partitioner "org.apache.cassandra.dht.RandomPartitioner"
    :reduce_cache_capacity_to 0.6
    :reduce_cache_sizes_at 0.85
    :request_scheduler "org.apache.cassandra.scheduler.NoScheduler"
    :rpc_keepalive true
    :rpc_port 9160
    :rpc_server_type "hsha"
    :saved_caches_directory "/mnt/cassandra/saved_caches"
    :seed_provider [{:class_name "org.apache.cassandra.locator.SimpleSeedProvider"
                     :parameters [{:seeds "127.0.0.1"}]}]
    :snapshot_before_compaction false
    :storage_port 7000
    :thrift_framed_transport_size_in_mb 15
    :thrift_max_message_length_in_mb 16}
   :service {:jmx-port 7199
             :jvm-opts default-jvm-opts}})


(def ^:const token-space
  "Maximum of the token space"
  (reduce * (repeat (bigint 127) (bigint 2))))

(defn tokens
  "Return tokens for a cassandra cluster of size count, evenly distributed over
token-space.  See http://wiki.apache.org/cassandra/Operations#Token_selection."
  [count]
  (let [j (/ token-space count)]
    (for [i (range count)]
      (bigint (* j i)))))

(defn initial-token
  "Compute node token.  If supplied-tokens is nil, then tokens are equi-spaced
over the token-space based on the number of nodes in the group."
  [supplied-tokens token-index]
  (if supplied-tokens
    (get supplied-tokens (primary-ip (target-node)))
    (let [nodes (map primary-ip (nodes-in-group))
          self-ip (primary-ip (target-node))
          node-tokens (zipmap nodes (tokens (count nodes)))]
      (str (+ (get node-tokens self-ip 0) (or token-index 0))))))

(defn seeds-list
  "Calculate the seed nodes"
  [max-seeds seeds]
  (let [seeds (or seeds
                  (map
                   #(or (private-ip %) (primary-ip %))
                   (take max-seeds (nodes-in-group))))]
    (string/join "," seeds)))

;;; ### Endpoint Snitch
(defmulti endpoint-snitch
  (fn [] (:provider (service-properties (compute-service)))))

(defmethod endpoint-snitch :default
  []
  "org.apache.cassandra.locator.SimpleSnitch")

(defmethod endpoint-snitch :aws-ec2
  []
  "org.apache.cassandra.locator.Ec2Snitch")

;;; ### rpc_address
(defmulti rpc-address-algo
  (fn [] (:provider (service-properties (compute-service)))))

(defmethod rpc-address-algo :default
  []
  :public)

(defmethod rpc-address-algo :aws-ec2
  []
  :private)

(defmulti node-rpc-address
  (fn [algo node] algo))

(defmethod node-rpc-address :public
  [_ node]
  (or (primary-ip node) (private-ip node)))

(defmethod node-rpc-address :private
  [_ node]
  (or (private-ip node) (primary-ip node)))

(defmethod node-rpc-address :all
  [_ node]
  "0.0.0.0")

(defn computed-settings
  "Update settings with cluster specific values."
  [{:keys [max-seeds service ram-fraction]
    :or {ram-fraction 0.4}
    :as settings}]
  ;; note there are two java processes, so ram-fraction should be less than
  ;; 0.5, as the ram settings are per JVM
  {:pre [(<= ram-fraction 0.5)]}
  (let [{:keys [config seeds token token-index]} service
        {:keys [ram] :as hw} (hardware (target-node))
        ram (- ram 100)                 ; allow the OS some ram
        ip (or (private-ip (target-node)) (primary-ip (target-node)))
        algo (or (:rpc-address-algo settings) (rpc-address-algo))
        defaults {:rpc_address (node-rpc-address algo (target-node))
                  :listen_address ip
                  :endpoint_snitch (endpoint-snitch)
                  :initial_token (initial-token tokens token-index)}
        default-sizes {:max-heap (format "%sM" (int (* ram-fraction ram)))
                       :heap-new (format "%sM" (int (* 0.2 ram-fraction ram)))
                       :young-gen-size (format "%sM"
                                               (int (* 0.1 ram-fraction ram)))
                       :stack-size "200K"}]
    (debugf "computed-settings defaults %s" defaults)
    (debugf "computed-settings hardware %s default-sizes %s"
            (pr-str hw) default-sizes)
    (->
     settings
     (update-in [:server]
                #(-> (merge defaults %)
                     (deep-merge config)
                     (assoc-in [:seed_provider 0 :parameters 0 :seeds]
                               (seeds-list max-seeds seeds))))
     (update-in [:service] #(merge default-sizes %)))))

(defmulti-version-plan settings-map [version settings])

(defmethod-version-plan
    settings-map {:os :debian-base}
    [os os-version version settings]
  (cond
   (:install-strategy settings) settings
   :else (assoc settings
           :install-strategy :package-source
           :package-source
           {:name "cassandra"
            :aptitude
            {:url "http://www.apache.org/dist/cassandra/debian"
             :release (str (string/join "" (take 2 version)) "x")
             :scopes ["main"]
             :key-server "pgp.mit.edu"
             :key-id "2B5C1B00"}}
           :packages ["cassandra"])))

(defplan settings
  "Settings for cassandra

`:server`
: a map of options as specified by
  [cassandra](http://www.datastax.com/docs/1.2/configuration/node_configuration).
  These will be serialised to yaml.  The `:endpoint_snitch` defaults to
  `SimpleSnitch` except on EC2, where `Ec2Snitch` is used.

`:service`
: a map of options for the jvm processes used to run cassandra.  The options to
  control memory, `:max-heap`, `:heap-new`, `:young-gen-size`, and `:stack-size`
  are defaulted based on the size of the node.  The algorithm is controlled by
  the `:ram-fraction` option.  `:jmx-port` and `:jvm-opts` are other supported
  keys.

`:ram-fraction`
: controls the amount of ram allocated to the cassandra processes.  It is a
  fraction that is applied to the node's installed total ram (less 100K for the
  OS). There are two cassandra processes, so the value should be less than
  0.5. It defaults to 0.4.

`:config-dir`
: the directory used to write the configuration files.

`:owner`
: the owner for cassandra files and directories

`:group`
: the group for cassandra files and directories

`:user`
: the user to run the cassandra service

`:service-name`
: the supervision service name

`:supervisor`
: the supervision service implementation (default :initd)

`:version`
: the cassandra version to install

`:group-data-centers`
: a map from group name to vectors of data center and rack names

`:max-seeds`
: the maximum number of seed nodes to use

`:rpc-address-algo`
: a keyword specifying which ip address to use for rpc_address.  Valid values
  are :private, :public and :all (which binds 0.0.0.0)."
  [{:keys [server] :as settings} {:keys [instance-id] :as options}]
  (let [settings (deep-merge (default-settings) settings)
        settings (settings-map (:version settings) settings)
        settings (computed-settings settings)]
    (assoc-settings :cassandra settings {:instance-id instance-id})))

;;; # Install
(defplan install
  "Install cassandra."
  [{:keys [instance-id]}]
  (let [{:keys [owner group server] :as settings} (get-settings
                                                   :cassandra
                                                   {:instance-id instance-id})
        {:keys [data_file_directories
                commitlog_directory
                saved_caches_directory]} server]
    (debugf "Install cassandra settings %s" settings)
    (crate-install/install :cassandra instance-id)
    (doseq [path (conj data_file_directories
                       commitlog_directory
                       saved_caches_directory)]
      (directory path :owner owner :group group :mode "0755"))))

;;; # Configure
(defplan config-file
  "Helper to write config files"
  [{:keys [owner group config-dir] :as settings} filename file-source]
  (directory config-dir :owner owner :group group)
  (apply
   remote-file (fragment (file ~config-dir ~filename))
   :flag-on-changed cassandra-config-changed-flag
   :owner owner :group group
   (apply concat file-source)))

(defn environment
  "cassandra-env.sh file content."
  [max-heap heap-new stack-size young-gen-size jmx-port jvm-opts]
  (let [vars {:MAX_HEAP_SIZE max-heap
              :HEAP_NEWSIZE heap-new
              :STACK_SIZE stack-size
              :YOUNG_GEN_SIZE young-gen-size
              :JMX_PORT jmx-port
              :JVM_OPTS (str "\\${JVM_OPTS} " (string/join " " jvm-opts))}]
    (apply str (map #(format "%s=\"%s\"\n" (name (key %)) (val %)) vars))))


(defn topology-properties
  "cassandra-topology.properties content.  group-data-centers is a map from
  group name to a vector with data-center and rack names.  This implies a group
  is rack specific.
  See http://www.datastax.com/docs/0.8/cluster_architecture/cluster_planning#configuring-the-propertyfilesnitch"
  [group-data-centers]
  (string/join
   "\n"
   (mapcat
    (fn [[group [dc-name rack-name]]]
       (for [node (nodes-in-group group)]
         (format "%s=%s:%s" (primary-ip node) dc-name rack-name)))
     group-data-centers)))



;;; ## Nodetool repair
;;; http://wiki.apache.org/cassandra/NodeTool
;;; http://wiki.apache.org/cassandra/Operations#Frequency_of_nodetool_repair
(defn cron-time
  "Return a vector with a minute and an hour at which to perform an operation.
Assumes an operation should be run once a day, and tries to spread the
operation for index'th of n operations over the course of a Sunday."
  [index n]
  {:pre [(number? n) (number? index) (< index n)]}
  (debugf "cron-time index %s n %s" index n)
  (let [maxq (inc (quot n 23))]
    [(* (/ 60 maxq) (quot index 23))
     (rem index 23)]))

(defn repair-cronjob-cron-file
  "Run repair on a node in a group, staggering when the repair is run.  The day
  of the week to run the repair can be chosed with day."
  [{:keys [keyspace day] :or {day 0 keyspace ""}}]
  (debugf "repair-cronjob-cron-file")
  (let [node (target-node)
        nodes (nodes-in-group)
        index (->> (map vector nodes (range))
                   (some #(when (= node (first %)) (second %))))
        _ (debugf "repair-cronjob-cron-file index %s count %s"
                  index (count nodes))
        [min hour] (cron-time index (count nodes))]
    (debugf "repair-cronjob-cron-file min %s hour %s" min hour)
    (format
      "%s\n%s %s * * %s root nodetool -h localhost repair -pr %s %s"
      "MAILTO=\"\"\nSHELL=/bin/bash"
      min hour day keyspace
      ">> /dev/null 2>&1")))

(defn repair-cronjob
  "Create a cronjob to run the nodetool repair"
  []
  (debugf "repair-cronjob")
  (remote-file "/etc/cron.d/cassandra-repair"
               :literal true
               :owner "root"
               :group "root"
               :content (repair-cronjob-cron-file {})))

;;; hack to compute the group name - there must be a better way to get this
;;; from jclouds
(defn ec2-region [node]
  (.. node getLocation getParent getId))

(defn jclouds-group []
  (str "jclouds#" (name (group-name))))

(defmulti open-ports
  "Open ports for cassandra"
  (fn [] (:provider (service-properties (compute-service)))))

(defmethod open-ports :default [])

(defmethod open-ports :aws-ec2
  []
  (require '[org.jclouds.ec2.security-group2])
  (let [authorize (ns-resolve 'org.jclouds.ec2.security-group2 'authorize)
        group (jclouds-group)]
    (debugf "open-ports group %s port %s udp" group 9160)
    (try
      (authorize (compute-service) group 9160 :protocol :udp)
      (catch Exception e
        (debugf "While changing security group: %s" (.getMessage e))))))

(defplan configure
  "Configure cassandra."
  [{:keys [instance-id] :as options}]
  (let [{:keys [group-data-centers service server] :as settings}
        (get-settings :cassandra {:instance-id instance-id})
        {:keys [max-heap heap-new stack-size young-gen-size
                jmx-port jvm-opts]} service]
    (debugf "config")
    (repair-cronjob)
    (debugf "config group-data-centers %s" group-data-centers)
    (when group-data-centers
      (config-file
       settings "cassandra-topology.properties"
       {:content (topology-properties group-data-centers)}))
    (debugf "config cassandra-env")
    (config-file settings "cassandra-env.sh"
                 {:content (environment
                            max-heap heap-new stack-size young-gen-size
                            jmx-port jvm-opts)})
    (debugf "config cassandra.yaml")
    (config-file settings "cassandra.yaml" {:content
                                            (yaml/generate-string server)})
    (open-ports)))

;;; # Run
(defplan service
  "Run the cassandra service."
  [& {:keys [action if-flag if-stopped instance-id]
      :or {action :manage}
      :as options}]
  (let [{:keys [supervision-options] :as settings}
        (get-settings :cassandra {:instance-id instance-id})]
    (service/service settings (merge supervision-options
                                     (dissoc options :instance-id)))))

(defn server-spec
  "Returns a server spec to install and configure a cassandra server spec."
  [settings & {:keys [instance-id] :as options}]
  (api/server-spec
   :phases {:settings (plan-fn
                        (pallet.crate.cassandra/settings settings options))
            :install (plan-fn (install options))
            :configure (plan-fn (configure options))}))

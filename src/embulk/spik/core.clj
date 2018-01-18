(ns embulk.spik.core
  (:require [embulk.spik.Repl :refer [embed command-line]]
            ;; [embulk.spik.restful :as rest]
            [embulk.spik.Tracker :refer [spik-module]]
            [clojure.spec.alpha :as s]
            [cheshire.core :refer [parse-string encode]]
            [clojure.set :refer [rename-keys]]
            [clj-http.client :as http] ; fixme: remove
            [yaml.core :as yaml])
  (:import [java.util Map ArrayList Map$Entry AbstractMap$SimpleEntry]
           com.google.inject.Module
           [org.embulk.config Config ConfigSource Task TaskSource DataSource DataSourceImpl ConfigException]
           [org.embulk.spi InputPlugin Schema Schema$Builder Exec PageBuilder ColumnVisitor]
           [org.embulk.spi.type Types]
           [org.embulk.spi.json JsonParser]
           [org.embulk.plugin InjectedPluginSource]
           [org.embulk EmbulkEmbed EmbulkRunner]
           org.embulk.EmbulkEmbed$Bootstrap
           java.nio.file.Paths))

(def ^:dynamic *schema*)
(def ^:dynamic *task*)
(def ^:dynamic *output*)
(def ^:dynamic *task-index*)

;; (defn map-to-msgpack-json [data]
;;   (.parse (JsonParser.) (chesire/encode data)))

(declare make-plugin)

(def selected-schema (atom nil))
(def selected-runner (atom nil))

(defn var-in-callee
  ([var-name namespace]
   ;; (println "Called var-in-callee " var-name " with ns:" namespace)
   (try
     (var-get (ns-resolve namespace (symbol var-name)))
     (catch NullPointerException e (throw (IllegalArgumentException. (str "Missing definition of " namespace "/" var-name))))))
  ([var-name]
   ;; (println "Called var-in-callee without ns" *ns*)
   (var-in-callee var-name *ns*)
   ;; (var-get (ns-resolve *ns* (symbol var-name)))
   )
  )

(defn section-of [config-key config-def]
  (or (-> (apply hash-map (drop-last config-def)) config-key :section)
      "connection"))

(defn stringify
  [keyword]
  (if (map? keyword)
    (into {} (for [[k v] keyword] [(subs (str k) 1) v]))
    (subs (str keyword) 1)))

(defn keywordize [string-map]
  (into {} (for [[k v] string-map] [(keyword k) v])))

(defn extract-show-if [configs-def]
  (let [property-entries (partition 2 (drop-last configs-def))
        contained-show-if-entries (filter #(contains? (nth % 1) :showIf) property-entries)
        show-if-plucked-entries (map (fn [pair] [(nth pair 0) ((nth pair 1) :showIf)]) contained-show-if-entries)
        section-grouped (group-by (fn [entry] (section-of (nth entry 0) configs-def)) show-if-plucked-entries)]
    section-grouped
    (into {} (for [[group show-ifs] section-grouped] [group {:showIf (into {} show-ifs)}]))
    ;; (mapv #(apply hash-map %) section-grouped)
    )
  )

(defn extract-required-if [configs-def]
  (let [property-entries (partition 2 (drop-last configs-def))
        contained-required-if-entries (filter #(contains? (nth % 1) :requiredIf) property-entries)
        required-if-plucked-entries (map (fn [pair] [(nth pair 0) ((nth pair 1) :requiredIf)]) contained-required-if-entries)
        section-grouped (group-by (fn [entry] (section-of (nth entry 0) configs-def)) required-if-plucked-entries)]
    section-grouped
    (into {} (for [[group required-ifs] section-grouped] [group {:requiredIf (into {} required-ifs)}]))
    ;; (mapv #(apply hash-map %) section-grouped)
    )
  )

(defn sync-dcs [dcs-project-path]
  "Emit out the data-connector-schema's integration file"
  (let [catalog-file-path (str dcs-project-path "/schemas/integration/" (str *ns*) ".yml")
        integration-schema (yaml/from-file catalog-file-path)]
    (spit catalog-file-path
          (yaml/generate-string
           (let [properties-updated (reduce
                                     (fn [acc pair]
                                       (let [config-key (nth pair 0)
                                             config-opt (nth pair 1)]
                                         (assoc-in acc
                                                   [(section-of config-key (var-in-callee "--configs")) "properties" (subs (str config-key) 1)]
                                                   (stringify (select-keys config-opt [:title :type])))))
                                     integration-schema
                                     (partition 2 (drop-last (var-in-callee "--configs"))))
                 inter-config (last (var-in-callee "--configs"))
                 section-groups (group-by #(section-of % (var-in-callee "--configs")) (inter-config :req))
                 require-updated (reduce (fn [acc group-name]
                                           (assoc-in acc
                                                     [group-name "required"]
                                                     (get section-groups group-name)))
                                         properties-updated
                                         (keys section-groups))
                 showIf-updated (merge-with merge require-updated (extract-show-if (var-in-callee "--configs")))
                 requiredIf-updated (merge-with merge showIf-updated (extract-required-if (var-in-callee "--configs")))
                 ]
             requiredIf-updated)
           :dumper-options {:flow-style :block}
           ))))

(concat [1 2 3] '(4 5 6))

(def keyword-to-schema-type-map
  {:string Types/STRING
   :long Types/LONG
   :double Types/DOUBLE
   :boolean Types/BOOLEAN
   :json Types/JSON
   :timestamp Types/TIMESTAMP}
  )

(interpose :json '(a b c))

(defn debug [label value]
  (println label " = " value)
  value)

(defn make-schema
  ([resdef]
   (make-schema resdef false))
  ([resdef with-association]
   (.build (reduce (fn [builder entry] (.add builder
                                             (stringify (entry 0))
                                             ((entry 1) keyword-to-schema-type-map)))
                   (Schema$Builder.)
                   (map vec (partition
                             2
                             (if with-association
                               (debug "concat" (vec (concat (:attr resdef) (apply concat (map vector (map keyword (:associations resdef)) (repeat :json))))))
                               (:attr resdef))))))))

;; (make-schema [:id :string
;;               :userId :string
;;               :body :string
;;               :title :string])

(def huyle-schema (-> (Schema$Builder.)
                      (.add "id" Types/STRING)
                      (.add "userId" Types/STRING)
                      (.add "body" Types/STRING)
                      (.add "title" Types/STRING)
                      .build))
(def dump-schema (-> (Schema$Builder.)
                     (.add "justtest" Types/STRING)
                     (.add "userId" Types/STRING)
                     (.add "body" Types/STRING)
                     (.add "title" Types/STRING)
                     .build))

(defn path [string]
  (if (nil? string)
    nil
    (Paths/get string (make-array String 0))))

(defmacro defplugin [name]
  `(def ~(symbol "--plugin-name") ~name))

(defn is-resdef? [obj]
  ;; TODO: improve this detection
  (contains? obj :name))

;; (defmacro defschema-associations [& body]
;;   "Receive a `org.embulk.spi.Schema` or a `resource-def`"
;;   `(defn ~(symbol "--schema-creator") []
;;      (let [result# (do ~@body)]
;;        (if (is-resdef? result#)
;;          (reset! selected-schema (make-schema result# true))
;;          (throw (RuntimeException. "Unsupported schema creator"))))))

(defmacro defschema [body & opts]
  "Receive a `org.embulk.spi.Schema` or a `resource-def`"
  (let [opts-map (into {} (map vec (partition 2 opts)))]
    `(defn ~(symbol "--schema-creator") []
       (let [result# ~body]
         (if (is-resdef? result#)
           (reset! selected-schema (make-schema result# ~(:associations opts-map)))
           (throw (RuntimeException. "Unsupported schema creator")))))))

(defn create-spik-module [namespace]
  (reify Module
    (configure [this binder]
      (InjectedPluginSource/registerPluginToInstance
       binder
       InputPlugin
       (str namespace)
       ;; (var-in-callee "--plugin-name" namespace)
       (make-plugin namespace)))))

(defmacro defrun [& body]
  (reset! spik-module (create-spik-module *ns*))
  `(defn ~(symbol "--runner") []
     ~@body))

(defn make-data-source [data]
  (reify
    ConfigSource
    (getAttributeNames [this]
      (ArrayList. (map #(subs (str %) 1) (keys this)))
      )  ; List<String>

    (getAttributes [this]
      (map #(AbstractMap$SimpleEntry. (% 0) (% 1)) data)  ;; not really a JsonNode
      ) ; Iterable<Map.Entry<String, JsonNode>>

    (isEmpty [this]
      (empty? data))

    (has [this attrName]
      (contains? (str ":" attrName)))

    (get
        [this type attrName]
      (get data (keyword (str ":" attrName))))

    (get [this type attrName defaultValue]
      (get data (keyword (str ":" defaultValue))))

    (getNested [this attrName]
      (make-data-source (.get this attrName)))

    (getNestedOrSetEmpty [this attrName]
      (throw (UnsupportedOperationException. "Unimplemented"))
      )

    (set [this attrName value]
      (throw (UnsupportedOperationException. "Unimplemented"))
      )

    (setNested [this attrName dataSource]
      (throw (UnsupportedOperationException. "Unimplemented"))
      )

    (setAll [this other]
      (throw (UnsupportedOperationException. "Unimplemented"))
      )

    (remove [this attrName]
      (throw (UnsupportedOperationException. "Unimplemented"))
      )

    (deepCopy [this]
      this)

    (merge [this other]
      (throw (UnsupportedOperationException. "Unimplemented")))

    (getObjectNode [this]
      (throw (UnsupportedOperationException. "Unimplemented"))
      )

    (loadConfig [this type]
      data)

    TaskSource
    (loadTask [this type]
      data)

    Task
    (dump [this]
      (println "customized dump")
      (make-data-source data))

    )
  )

(defn load-config
  [data-source]
  (keywordize (.loadConfig data-source Map)))

(defn validate-config [configs]
  (when-not (s/valid? ::configs configs)
    (throw (ConfigException. (clojure.string/replace (s/explain-str ::configs configs) "In:" "\nIn:")))))

(defn spec-by-type [type]
  (type {:string clojure.core/string?
         :number clojure.core/number?}))

(defn spec-with-type [conf-props]
  (if (nil? (:spec conf-props))
    (spec-by-type (:type conf-props))
    (s/and (:spec conf-props) (spec-by-type (:type conf-props)))))

(defn update-spec-by-type [conf-props]
  (assoc conf-props :spec (spec-with-type conf-props)))

(defn qualifize
  [keywords]
  (map #(keyword "embulk.spik.core" (subs (str %) 1)) keywords))

(defmacro defconfigs [& configs]
  ;; (println "Define macro with configs: " configs)
  (let [whole-spec (rename-keys (last configs) {:req :req-un :opt :opt-un})
        ;; This may has a subtle ordering issue with `conj`
        pre-confs (partition 2 (conj configs whole-spec :configs))
        confs (map (fn [[key props]]
                     (if (= :configs key)
                       [key props]
                       [key (update-spec-by-type props)])) pre-confs)]
    `(do
       (def ~(symbol "--configs") '~configs)
       ~@(map
          (fn [conf] (let [conf-name (subs (str (nth conf 0)) 1)
                           qualified-config-keyword (keyword "embulk.spik.core" conf-name)
                           spec (if (= qualified-config-keyword ::configs)
                                  (nth conf 1)
                                  (spec-with-type (nth conf 1)))]
                       (if (= qualified-config-keyword ::configs)
                         (let [qualified-inner-keys (map (fn [[k v]] [k (qualifize v)]) spec)]
                           (do
                             ;; (println "Apply ::configs" `(s/def ~qualified-config-keyword
                             ;;                               (s/keys ~@(apply concat (seq qualified-inner-keys)))))
                             `(s/def ~qualified-config-keyword
                                (s/keys ~@(apply concat (seq qualified-inner-keys)))))
                           )
                         `(s/def ~qualified-config-keyword ~spec))))
          confs))
    ))

(defn load-task
  [data-source]
  (keywordize (.loadTask data-source Map)))

(defn wrap-data-source [original-data-source]
  (make-data-source (keywordize (parse-string (.toString original-data-source)))))

(defn default-transaction [plugin config-source control]
  (validate-config (load-config config-source))
  ;; FIXME: analyze how to TaskSource being consumed downstream
  (.resume plugin (wrap-data-source config-source) *schema* 1 control))

(defn make-plugin [running-namespace]
  (reify InputPlugin
    (transaction [this config-source control]
      ;; (println "config-source" (wrap-data-source config-source))
      (binding [*schema* huyle-schema
                *task* (keywordize (load-config config-source))]
        (default-transaction this config-source control)))
    (resume [this task-source schema task-count control]
      (binding [*task* (keywordize (load-task task-source))]
        (println "resume " running-namespace)
        ((var-in-callee "--schema-creator" running-namespace)))
      (.run control task-source @selected-schema task-count)
      ;; TODO: config-diff should be fetched before or after control
      ;; TODO: implement config diff / incremental
      (Exec/newConfigDiff))
    (cleanup [this task schema index output]
      ;; Do nothing
      )
    (guess [this config]
      (Exec/newTaskReport))

    (run [this task-source schema taskIndex output]
      (binding [
                *schema* huyle-schema
                *task* (keywordize (load-task task-source))
                ]
        (let [task (.loadConfig task-source Map)
              ;; response (http/get (str "https://www.googleapis.com/customsearch/v1?key=" (:apiKey task)
              ;;                         "&cx=" (:engineId task)
              ;;                         "&q=coding")
              ;;                    {:throw-entire-message? true})
              ]
          ;; (println "Loaded task: " task)
          (binding [*task* task
                    *schema* schema
                    *task-index* taskIndex
                    *output* output]
            ((var-in-callee "--runner" running-namespace)))
          ;; (def captured-response response)
          ;; (consume-response response schema output)
          (Exec/newTaskReport))))))

(defn start []
  ;; (println "Register plugin:" (var-in-callee "--plugin-name" *ns*))
  (let [module (create-spik-module *ns*)]
    (when (not (nil? @embed))
      (.destroy @embed))
    (reset! embed nil)
    (let [bootstrap (-> (EmbulkEmbed$Bootstrap.)
                        ;; (.addModules [module])
                        )
          embed (.initialize bootstrap)
          runner (EmbulkRunner. embed)
          command-line @command-line]
      (.run runner
            (path (-> command-line .getArguments (.get 0)))
            (path (.getConfigDiff command-line))
            (path (.getOutput command-line))
            (path (.getResumeState command-line))))))

(defn consume-response [records resdef opts-map]
  (let [page-builder (PageBuilder. (Exec/getBufferAllocator) @selected-schema *output*)]
    (doseq [record records]
      (doseq [[[attr-key type] index] (map
                                       ;; (fn [[attr-key type] index]
                                       ;;   (println "Do set columns")
                                       ;;   (case type
                                       ;;     :string (.setString page-builder index (attr-key record))
                                       ;;     :boolean (.setBoolean page-builder index (attr-key record))
                                       ;;     :long (.setLong page-builder index (attr-key record))
                                       ;;     :double (.setDouble page-builder index (attr-key record))
                                       ;;     :json (.setDouble page-builder index (attr-key record))
                                       ;;     :null (.setNull page-builder index (attr-key record))))
                                       vector
                                       (partition 2 (vec (concat (:attr resdef) (if (:associations opts-map)
                                                                                  (apply concat (map vector (map keyword (:associations resdef)) (repeat :json)))
                                                                                  []))))
                                       (range))]
        (case type
          :string (.setString page-builder index (str (get record attr-key)))
          :boolean (.setBoolean page-builder index (get record attr-key))
          :long (.setLong page-builder index (get record attr-key))
          :double (.setDouble page-builder index (get record attr-key))
          :json (.setJson page-builder index (.parse (JsonParser.) (encode (get record attr-key))))
          :null (.setNull page-builder index (get record attr-key)))
        )                               ;
      (.addRecord page-builder)
      (.flush page-builder)
      )
    ))


(defmacro pumper [& body]
  `(do
     (defschema ~@body)
     (defrun (~(resolve 'embulk.spik.restful/pump) ~@body))
     )
  )

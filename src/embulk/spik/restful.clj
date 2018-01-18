(ns embulk.spik.restful
  (:require [clojure.zip :as zip]
            [clojure.data.zip :refer [right-locs]]
            [clojure.test :refer [deftest is]]
            [clojure.string :as string]
            [clj-http.client :as client]
            [embulk.spik.core :refer [*task* *schema* *output* *task-index*
                                      make-schema
                                      selected-schema
                                      consume-response]]
            [cheshire.core :as json])
  (:import [org.embulk.spi InputPlugin Schema Schema$Builder Exec PageBuilder ColumnVisitor]
           ))

(defn join-paths [paths]
  "Assume the first one in the path is `/` symbol"
  (string/join "/" (subvec paths 1)))

;; (defn attr [resdef]
;;   (let [sec (second resdef)]
;;     (if (map? sec) sec {})))
;; (attr '(posts {:a 1} (:id (comments))))

(defn ordered-map [vector]
  (map vec (partition 3 vector)))

(defn children [resdef]
  (let [sec (second resdef)]
    (if (map? sec)
      (nth resdef 2 '())
      sec)))

(defn path-to [node-zipper]
  (conj (reduce (fn [path node]
                  (if (seq? (first node))
                    path
                    (conj path (first node))))
                []
                (zip/path node-zipper))
        (zip/node (zip/down node-zipper))))

(defn zsubnodes [node-loc]
  (let [z1 (zip/down node-loc)
        z2 (zip/right z1)
        z3 (zip/right z2)
        has-attr? (and (not (nil? z2))
                       (vector? (zip/node z2)))]
    (if has-attr? (right-locs (zip/right z2)) (right-locs (zip/right z1)))))

(defn associations [nodez]
  (apply concat (for [subz (zsubnodes nodez)]
                  (do
                    ;; (println "subz: " (zip/node subz))
                    (if (= :id (zip/node (zip/down subz)))
                      (map
                       (comp zip/node zip/down)
                       (zsubnodes subz))
                      '())))))

(defn enrich-node [respec base-url]
  (assoc respec
         :url (str base-url (join-paths (:path respec)))))

(defn enrich-nodes [entries base-url]
  (map (fn [[resource-name resource-spec]]
         [resource-name
          (enrich-node resource-spec base-url)])
       entries))

(defn process-node [node-zipper]
  (let [z1 (zip/down node-zipper)
        z2 (zip/right z1)
        z3 (zip/right z2)
        has-attr? (and (not (nil? z2))
                       (vector? (zip/node z2)))]
    [{:name (str (zip/node z1))
      :attr (if has-attr? (second (zip/node node-zipper)) {})
      :path (path-to node-zipper)
      :associations (associations node-zipper)}
     (zsubnodes node-zipper)]))

(def test-graph
  (zip/seq-zip '(/
                 (posts {:id :id
                         :userId :fid
                         :body :string
                         :title :string}
                        (:id (comments)))
                 (comments {})
                 (albums {}))))

;; (associations (-> test-graph
;;                   zip/down
;;                   zip/right))


;; (process-node (zip/seq-zip '(posts (comments) (related))))

(defn merge-entry [entry1 entry2]
  (println "WARN: Unimplemented merge-entry")
  entry2)

(defn update-entries
  [entries new-entry]
  (let [exist-entry (get entries (symbol (:name new-entry)))]
    (if (nil? exist-entry)
      (assoc entries (symbol (:name new-entry)) new-entry)
      (assoc entries (symbol (:name new-entry)) (merge-entry exist-entry new-entry)))))

(defn process-graph
  [entries node-zipper]
  (let [exploded-node (process-node node-zipper)
        node-spec (first exploded-node)
        subnodezs (second exploded-node)]
    (if (empty? subnodezs)
      (update-entries entries node-spec)
      (reduce (fn [acc-entries child]
                (process-graph acc-entries child))
              (update-entries entries node-spec)
              subnodezs))))

(defmacro defgraph [base-url & nodes]
  (let [entries (enrich-nodes (process-graph {} (zip/seq-zip (cons (symbol "/") nodes))) base-url)]
    `(do
       (def ~(symbol "graph") ~(into {} (map (fn [[res-name respec]]
                                               [(str res-name)
                                                (into {} (map (fn [[k v]]
                                                                (if (or (= k :children)
                                                                        (= k :associations)
                                                                        (= k :path))
                                                                  [k `(quote ~v)]
                                                                  [k v]))
                                                              respec
                                                              ))]
                                               )
                                             entries)))
       ~@(for [[resource-symbol spec] entries]
           `(def ~resource-symbol ~(into {} (map (fn [[k v]] (if (or (= k :children)
                                                                     (= k :associations)
                                                                     (= k :path))
                                                               [k `(quote ~v)]
                                                               [k v]))
                                                 spec)))))))

;; (enrich-nodes (process-graph {} test-graph) "https://jsonplaceholder.typicode.com/")

(defn fetch-associations
  [respec record]
  (let [associations
        (into {}
              (for [association (:associations respec)]
                [(keyword association) (json/parse-string
                                        (:body (client/get (str (:url respec) "/"
                                                                (:id record) "/"
                                                                association))))]))]
    (merge record associations)))

(defn params->query-string [m]
  (clojure.string/join "&" (for [[k v] m] (str (name k) "=" v))))

(defn GET
  ([respec]
   (GET respec {} {}))
  ([respec params]
   (GET respec params{}))
  ([respec params opts-map]
   (let [records (json/parse-string
                  (:body (client/get (if (empty? params)
                                       (:url respec)
                                       (str (:url respec) "?" (params->query-string params)))))
                  true)]
     (if (:associations opts-map)
       (map (partial fetch-associations respec) records)
       records))))

;; (defmacro pump [schema-selection & body]
;;   (reset! selected-schema (make-schema (var-get (resolve resdef))))
;;   (reset! selected-runner (make-schema resdef))

;;   `(do
;;      (def ~(symbol "--schema")
;;        ~(make-schema (var-get (resolve resdef)))
;;        )
;;      (defn ~(symbol "--runner") []
;;        (consume-response (GET ~resdef) ~resdef)))
;;   )

(defn pump [resdef & opts]
  "Execute HTTP GET request over the resource and pipe the response json to the output"
  (let [opts-map (into {} (map vec (partition 2 opts)))]
    (consume-response (apply GET resdef {} opts-map) resdef opts-map)
    )
  )

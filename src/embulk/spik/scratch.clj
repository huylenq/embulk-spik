(ns embulk.spik.scratch
  (:require [embulk.spik.restful :refer :all]
            [embulk.spik.core :as spik :refer :all]
            [clojure.spec.alpha :as s]))

(defplugin "huyle")

(defconfigs
  :apiKey {:title "The new API Key"
           :type :string
           :spec (s/and
                  (complement empty?)
                  string?)}

  :target {:type :string}

  :engineId {:title "The Engine ID"
             :type :number
             :section "import_basic"
             :spec (s/and (complement empty?)
                          (complement nil?))}

  :customFilter {:title "Custom filter"}

  :filterText {:title "Filter Text"
               :showIf {:customFilter true}}

  {:req [:apiKey :engineId]})

;; (sync-dcs "/Users/huyle/Projects/td/data-connector-schema")

(defgraph
  "https://jsonplaceholder.typicode.com/"
  (posts [:id :string
          :userId :string
          :body :string
          :title :string]
         (:id (comments)))
  (comments [:id :string
             :email :string
             :body :string])
  (albums []))

(defschema (get graph (:target *task*)))

(defrun (pump (get graph (:target *task*))))

;; (start)

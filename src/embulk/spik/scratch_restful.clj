(ns embulk.spik.scratch-restful
  (:require [embulk.spik.restful
             :as r
             :refer [defgraph
                     GET]]
            [clojure.zip :as zip]))

(defgraph
  "https://jsonplaceholder.typicode.com/"
  (posts {:id :id
          :userId :fid
          :body :string
          :title :string}
         (:id (comments)))
  (comments {})
  (albums {}))

;; (def got-posts (GET posts {:associations true}))
;; got-posts

;; (def sample-post (nth (GET posts) 0))
;; (r/fetch-associations posts sample-post)

;; (GET posts {:associations true})

;; (count (first (posts)))

;; (defgraph
;;   (posts {} (:id (comments)))
;;   (comments {})
;;   (albums {}))


(defmacro process [root]
  `(do ~@(for [child (zip/children ~root)]
           `(def ~child 1))))


(defmacro process [root]
  `(do ~@(for [child (zip/children root)]
           (let [path-node-name (first child)]
             `(def ~path-node-name (str ~path-node-name))))))

(enumerate-graph graph)

(get posts)



;; (def posts
;;   {:base jsonplaceholder-host}
;;   {:id :number
;;    :userId :numberck
;;    :title :string
;;    :body :string}
;;   )

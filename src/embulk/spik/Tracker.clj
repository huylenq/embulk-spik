(ns embulk.spik.Tracker
  (:require
   [clojure.tools.nrepl.server :refer [start-server stop-server]]))

(gen-class :name org.embulk.spik.Tracker)

(import org.embulk.spik.Tracker)
(gen-class
 :name org.embulk.spik.Tracker
 :methods [#^{:static true} [instance [] org.embulk.spik.Tracker]
           [captureIn [Class String] void]
           [captureIn [String String] void]
           [captureOut [Class String] void]
           [captureOut [String String] void]
           [spikModule [] Object]
           [restart [] void]])

(def timeline (atom []))
(def stack (atom []))

;; TODO: thread-safety
(defonce instance (Tracker.))

(defn -instance [] instance)

(defn -restart [self]
  (reset! timeline [])
  (reset! stack []))

(defn capture
  [event]
  ;; Timeline
  (println event)
  (swap! timeline #(conj % event))
  ;; Event Stacks
  ;; TODO: this can be done in a cleaner way
  (case (:direction event)
    :in (swap! stack #(conj % event))
    :out (swap! stack #(let [tail (last %)]
                         (if (and (= (:component tail) (:component event))
                                  (= (:event tail) (:event event)))
                           (pop %)
                           (throw (IllegalStateException.
                                   (str "Couldn't find the last 'in' event for " event))))))
    nil
    ))

;; (IllegalStateException. "Last frame on stack isn't the popping event!")

(defn -captureIn [self component event]
  (capture
   {:component component
    :event (symbol event)
    :direction :in
    :thread (.getName (Thread/currentThread))
    }))

(defn -captureOut [self component event]
  (capture
   {:component component
    :event (symbol event)
    :direction :out
    :thread (.getName (Thread/currentThread))}))

(def spik-module (atom nil))
(defn -spikModule [self]
  @spik-module
  )

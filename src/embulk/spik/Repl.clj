(ns embulk.spik.Repl
  (:require
   [clojure.tools.nrepl.server :refer [start-server stop-server]]
   [cider.nrepl :refer [cider-nrepl-handler]])
  (:gen-class
   :name org.embulk.spik.Repl
   :methods [^:static [start [] void]
             ^:static [stop [] void]
             ^:static [capture [Object] void]])
  )

(def embed (atom nil))
;; (defn embed []
;;   (when (nil? @atom))
;;   (throw (Exception. "`EmbulkEmbed` not found. Use Repl.capture() to capture it first!"))
;;   captured-embed)

(def command-line (atom nil))

(defn -capture [object]
  (let [class-name (.getName (class object))]
    ;; (case "com.google.inject.Injector" (reset! captured-injector object))
    (case class-name
      "org.embulk.EmbulkEmbed" (reset! embed object)
      "org.embulk.cli.EmbulkCommandLine" (reset! command-line object)
      )))

(defn -start []
  (defonce server (start-server :port 7888
                                :handler cider-nrepl-handler))
  (println "Started a REPL on port 7888"))

(defn -stop []
  (if (not (nil? server))
    (stop-server server)
    (println "It seems you didn't start any REPL server!")))


;; (InjectedPluginSource/registerPluginTo binder
;;                                        (class InputPlugin)
;;                                        "huyle-plugin"
;;                                        (class huyle-plugin))


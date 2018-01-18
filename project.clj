(defproject embulk-spik "0.1.0-SNAPSHOT"
  :description "Embulk Spik"
  :url "https://embulk.org"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [clojure-lanterna "0.9.7"]
                 [cider/cider-nrepl "0.16.0-SNAPSHOT"]
                 [com.google.inject/guice "4.0" :scope "provided"]
                 [org.clojure/data.zip "0.1.2"]
                 [clj-http "3.7.0"]
                 [cheshire "5.8.0"]
                 [clojure-term-colors "0.1.0-SNAPSHOT"]
                 [io.forward/yaml "1.0.6"]
                 ;; [org.embulk/embulk "0.8.36"]
                 ]
  :aot [embulk.spik.Repl embulk.spik.Tracker])

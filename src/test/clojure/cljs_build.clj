(ns cljs-build
  "Script to compile ClojureScript tests.
   Usage: clojure -M:cljs-test -m cljs-build [simple|advanced]"
  (:require [cljs.build.api :as api]))

;; Pre-load CLJC test namespaces on the CLJ side so that defsession
;; macros can resolve rule/query vars at compile time.
(require 'clara.test-common)

(def builds
  {"simple"   {:output-to     "target/js/test-simple.js"
               :target        :nodejs
               :main          'clara.test
               :optimizations :simple}
   "advanced" {:output-to          "target/js/test-advanced.js"
               :target             :nodejs
               :main               'clara.test
               :anon-fn-naming-policy :mapped
               :optimizations      :advanced
               :externs             ["src/test/clojurescript/externs.js"]}})

(defn -main [& args]
  (let [build-id (or (first args) "simple")
        opts     (get builds build-id)]
    (when-not opts
      (println "Unknown build:" build-id)
      (println "Available builds:" (keys builds))
      (System/exit 1))
    (println "Compiling ClojureScript" build-id "build...")
    (api/build
      (api/inputs "src/test/clojurescript" "src/test/common" "src/main/clojure")
      opts)
    (println "Done. Output:" (:output-to opts))))

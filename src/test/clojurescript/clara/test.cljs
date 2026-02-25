(ns clara.test
  (:require-macros [cljs.test :as test])
  (:require [clara.test-rules]
            [clara.test-rules-require]
            [cljs.test]
            [clara.test-salience]
            [clara.test-complex-negation]
            [clara.test-common]
            [clara.test-testing-utils]
            [clara.test-accumulators]
            [clara.test-exists]
            [clara.tools.test-tracing]
            [clara.tools.test-fact-graph]
            [clara.tools.test-inspect]
            [clara.test-truth-maintenance]
            [clara.test-dsl]
            [clara.test-accumulation]
            [clara.test-memory]
            [clara.test-simple-rules]
            [clara.test-rhs-retract]
            [clara.test-bindings]
            [clara.test-clear-ns-productions]
            [clara.test-negation]
            [clara.performance.test-rule-execution]
            [clara.test-node-sharing]
            [clara.test-queries]))

(defmethod cljs.test/report [:cljs.test/default :end-run-tests] [m]
  (if (cljs.test/successful? m)
    (do
      (println "Success!")
      (.exit js/process 0))
    (do
      (println "FAIL")
      (.exit js/process 1))))

(defn -main []
  (test/run-tests 'clara.test-rules
                  'clara.test-rules-require
                  'clara.test-common
                  'clara.test-salience
                  'clara.test-testing-utils
                  'clara.test-complex-negation
                  'clara.test-accumulators
                  'clara.test-exists
                  'clara.tools.test-tracing
                  'clara.tools.test-fact-graph
                  'clara.tools.test-inspect
                  'clara.test-truth-maintenance
                  'clara.test-dsl
                  'clara.test-accumulation
                  'clara.test-memory
                  'clara.test-simple-rules
                  'clara.test-rhs-retract
                  'clara.test-bindings
                  'clara.test-clear-ns-productions
                  'clara.test-negation
                  'clara.performance.test-rule-execution
                  'clara.test-node-sharing
                  'clara.test-queries))

(set! *main-cli-fn* -main)

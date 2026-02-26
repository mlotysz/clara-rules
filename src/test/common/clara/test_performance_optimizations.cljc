;; These tests validate that operations that the rules engine should optimize
;; away are in fact optimized away.  The target here is not the actual execution time,
;; which will vary per system, but verification that the action operations in question are not performed.
#?(:clj
   (ns clara.test-performance-optimizations
     (:require [clara.tools.testing-utils :refer [def-rules-test
                                                  side-effect-holder] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query]]

               [clara.rules.testfacts :refer [->Cold ->ColdAndWindy ->Temperature ->WindSpeed]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Cold
               ColdAndWindy
               Temperature
               WindSpeed]))

   :cljs
   (ns clara.test-performance-optimizations
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query]]
               [clara.rules.testfacts :refer [->Cold Cold
                                              ->ColdAndWindy ColdAndWindy
                                              ->Temperature Temperature
                                              ->WindSpeed WindSpeed]]
               [clara.rules.accumulators]
               [cljs.test]
               [schema.test :as st]
               [clara.tools.testing-utils :refer [side-effect-holder] :as tu])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))
(use-fixtures :each tu/side-effect-holder-fixture)

#?(:clj
   (defmacro true-if-binding-absent
     []
     (not (contains? &env '?unused-binding))))

;; See issue https://github.com/cerner/clara-rules/issues/383
;; This validates that we don't create let bindings for binding
;; variables that aren't used.  Doing so both imposes runtime costs
;; and increases the size of the generated code that must be evaluated.
(def-rules-test test-unused-rhs-binding-not-bound

  {:rules [cold-windy-rule [[[ColdAndWindy (= ?used-binding temperature) (= ?unused-binding windspeed)]]
                            (when (true-if-binding-absent)
                              (insert! (->Cold ?used-binding)))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]]

   :sessions [empty-session [cold-windy-rule cold-query] {}]}

  (is (= [{:?c 0}]
         (-> empty-session
             (insert (->ColdAndWindy 0 0))
             fire-rules
             (query cold-query)))))

;; Test that sub-indexing for ExpressionJoinNode works correctly.
;; The pattern (= (:location ?t) location) uses a computed expression on a parent-bound
;; variable (?t), which triggers a non-equality unification and thus an ExpressionJoinNode.
;; With sub-indexing, the equality is extracted as an index key to avoid the full cross-product.
(def-rules-test test-expression-join-sub-indexing

  {:queries [temp-wind-query [[]
                              [[Temperature (= ?t this)]
                               [WindSpeed (= (:location ?t) location)
                                          (= ?w windspeed)]]]]

   :sessions [empty-session [temp-wind-query] {}]}

  (let [results (-> empty-session
                    (insert (->Temperature 10 "MCI")
                            (->Temperature 20 "LAX")
                            (->WindSpeed 30 "MCI")
                            (->WindSpeed 40 "LAX")
                            (->WindSpeed 50 "ORD"))
                    fire-rules
                    (query temp-wind-query))]
    ;; Should match MCI temp with MCI wind, and LAX temp with LAX wind.
    ;; ORD wind should not match any temperature.
    (is (= 2 (count results)))
    (is (= #{[10 30] [20 40]}
           (into #{} (map (juxt (comp :temperature :?t) :?w) results))))))

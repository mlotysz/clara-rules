(ns clara.bench
  "Standalone benchmark suite — 50 scenarios, ~100k facts each.

   Run via deps.edn alias:
     clojure -M:bench                    # default: N=100000, ITERS=3
     BENCH_N=10000 clojure -M:bench      # quick smoke test
     BENCH_ONLY=01,11,50 clojure -M:bench  # run specific scenarios

   Outputs a result table; save to file for comparison:
     clojure -M:bench | tee results-fork.txt
     clojure -M:bench | tee results-oracle.txt   # run same in oracle worktree
     diff results-fork.txt results-oracle.txt"
  (:require [clara.rules             :as r]
            [clara.rules.accumulators :as acc]
            [clara.rules.compiler    :as com]))

;;; ── Fact types ──────────────────────────────────────────────────────────────

(defrecord Order    [id customer-id amount category priority])
(defrecord Customer [id tier region credit-score])
(defrecord Shipment [id order-id warehouse status])
(defrecord Discount [order-id pct reason])
(defrecord Flag     [entity-id kind])
(defrecord Metric   [id name value unit])
(defrecord Alert    [id severity entity-id message])
(defrecord Tag      [entity-id label])
(defrecord Event    [id type entity-id ts])

;;; ── Config ──────────────────────────────────────────────────────────────────

(def ^:const N
  (Long/parseLong (or (System/getenv "BENCH_N") "100000")))

(def ^:const ITERS
  (Long/parseLong (or (System/getenv "BENCH_ITERS") "2")))

(def ONLY
  (when-let [v (System/getenv "BENCH_ONLY")]
    (let [ids (set (remove clojure.string/blank? (clojure.string/split v #",")))]
      (when (seq ids) ids))))

;;; ── Harness ─────────────────────────────────────────────────────────────────

(defn time-ms [f]
  (let [t0 (System/currentTimeMillis) _ (f)]
    (- (System/currentTimeMillis) t0)))

(defn bench-fn [f]
  (let [times (mapv (fn [_] (time-ms f)) (range ITERS))
        mean  (/ (double (reduce + times)) ITERS)]
    {:mean mean :min (reduce min times) :max (reduce max times)}))

(defn run-scenario [{:keys [id label productions opts facts-fn run-fn]}]
  (when (or (nil? ONLY) (ONLY id))
    (print (format "  [%s] %-64s" id label)) (flush)
    ;; mk-session takes ([sources...] options...) where each source is a seq of productions.
    ;; Wrap productions in a vector so they form one source.
    (let [mk-args (into [productions] (or opts []))
          session (com/mk-session mk-args)
          facts   (facts-fn)
          {:keys [mean min max]} (bench-fn #(run-fn session facts))]
      (println (format "mean=%7.1f  min=%5d  max=%5d  ms"
                       mean (long min) (long max)))
      {:id id :label label :mean mean :min min :max max})))

;;; ── Production helpers ──────────────────────────────────────────────────────

(defn mkr
  "Make a rule production map."
  [name lhs rhs]
  {:name name :lhs lhs :rhs rhs :ns-name 'clara.bench})

(defn mkq
  "Make a query production map."
  [name params lhs]
  {:name name :params params :lhs lhs :ns-name 'clara.bench})

;;; ── Scenarios ───────────────────────────────────────────────────────────────

(def scenarios
  [
   ;;; ─── Section 1: Alpha Network Throughput ─────────────────────────────────

   {:id "01" :label "alpha passthrough — 100k inserts, no rules"
    :productions []
    :facts-fn #(mapv (fn [i] (->Metric i "cpu" (rand) "pct")) (range N))
    :run-fn (fn [s facts] (r/insert-all s facts))}

   {:id "02" :label "alpha equality — 1% selectivity"
    :productions [(mkr "r" [{:type Metric :constraints ['(= name "cpu")]}] '(do nil))]
    :facts-fn #(mapv (fn [i] (->Metric i (if (< i (/ N 100)) "cpu" "mem") (rand) "pct")) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "03" :label "alpha range — 50% selectivity"
    :productions [(mkr "r" [{:type Metric :constraints ['(> value 0.5)]}] '(do nil))]
    :facts-fn #(mapv (fn [i] (->Metric i "cpu" (/ (mod i 100) 100.0) "pct")) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "04" :label "alpha triple constraint — 3 predicates combined"
    :productions [(mkr "r" [{:type Order
                              :constraints ['(= category "electronics")
                                            '(> amount 100)
                                            '(= priority :high)]}]
                        '(do nil))]
    :facts-fn #(mapv (fn [i] (->Order i (mod i 1000) (+ 50 (mod i 200))
                                      (rand-nth ["electronics" "books" "clothing"])
                                      (rand-nth [:high :medium :low])))
                     (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "05" :label "five independent rules — mixed fact types"
    :productions [(mkr "r1" [{:type Metric :constraints ['(= name "cpu")]}]   '(do nil))
                  (mkr "r2" [{:type Metric :constraints ['(= name "mem")]}]   '(do nil))
                  (mkr "r3" [{:type Order  :constraints ['(= category "electronics")]}] '(do nil))
                  (mkr "r4" [{:type Order  :constraints ['(= priority :high)]}] '(do nil))
                  (mkr "r5" [{:type Event  :constraints ['(= type "click")]}] '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Metric i (rand-nth ["cpu" "mem" "disk"]) (rand) "pct")) (range 40000))
                      (mapv (fn [i] (->Order  i (mod i 1000) 100.0 (rand-nth ["electronics" "books"]) :high)) (range 40000))
                      (mapv (fn [i] (->Event  i (rand-nth ["click" "view"]) (mod i 5000) i)) (range 20000))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "06" :label "alpha + fact binding — 100k matches"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(= name "cpu")]}] '(do nil))]
    :facts-fn #(mapv (fn [i] (->Metric i "cpu" (rand) "pct")) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "07" :label "ten alpha nodes — equality dispatch"
    :productions (vec (for [nm ["cpu" "mem" "disk" "net" "gpu" "io" "swap" "temp" "fan" "pwr"]]
                        (mkr (str "r-" nm) [{:type Metric :constraints [(list '= 'name nm)]}] '(do nil))))
    :facts-fn (let [names ["cpu" "mem" "disk" "net" "gpu" "io" "swap" "temp" "fan" "pwr"]]
                #(mapv (fn [i] (->Metric i (rand-nth names) (rand) "pct")) (range N)))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "08" :label "insert 100k facts — no fire-rules called"
    :productions [(mkr "r" [{:type Metric :constraints ['(= name "cpu")]}] '(do nil))]
    :facts-fn #(mapv (fn [i] (->Metric i (rand-nth ["cpu" "mem"]) (rand) "pct")) (range N))
    :run-fn (fn [s facts] (r/insert-all s facts))}

   {:id "09" :label "twenty alpha nodes — even distribution"
    :productions (vec (for [n (range 20)]
                        (mkr (str "r" n) [{:type Metric :constraints [(list '= 'name (str n))]}] '(do nil))))
    :facts-fn #(mapv (fn [i] (->Metric i (str (mod i 20)) (rand) "pct")) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "10" :label "insert 50k + retract 50k + fire"
    :productions [(mkr "r" [{:type Metric :constraints ['(= name "cpu")]}] '(do nil))]
    :facts-fn #(hash-map
                :a (mapv (fn [i] (->Metric i "cpu" (rand) "pct")) (range (/ N 2)))
                :b (mapv (fn [i] (->Metric (+ i (/ N 2)) "mem" (rand) "pct")) (range (/ N 2))))
    :run-fn (fn [s {:keys [a b]}]
              (let [s2 (r/insert-all s (concat a b))]
                (-> (apply r/retract s2 b) r/fire-rules)))}

   ;;; ─── Section 2: Hash Joins ───────────────────────────────────────────────

   {:id "11" :label "hash join 1:1 — 50k×50k all match"
    :productions [(mkr "r" [{:type Order    :fact-binding :?o :constraints []}
                              {:type Customer :fact-binding :?c :constraints ['(= id (:customer-id ?o))]}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :gold "US" 700)) (range (/ N 2)))
                      (mapv (fn [i] (->Order i i 150.0 "electronics" :high)) (range (/ N 2)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "12" :label "hash join 1:many — 1k customers × ~99 orders"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints []}
                              {:type Order    :fact-binding :?o :constraints ['(= customer-id (:id ?c))]}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :silver "US" 650)) (range 1000))
                      (mapv (fn [i] (->Order i (mod i 1000) 100.0 "books" :medium)) (range (- N 1000)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "13" :label "hash join sparse — 10% match rate"
    :productions [(mkr "r" [{:type Order    :fact-binding :?o :constraints []}
                              {:type Customer :fact-binding :?c :constraints ['(= id (:customer-id ?o))]}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :bronze "EU" 600)) (range (/ N 10)))
                      (mapv (fn [i] (->Order i (mod i N) 50.0 "clothing" :low)) (range N))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "14" :label "three-way hash join — Order×Customer×Shipment"
    :productions [(mkr "r" [{:type Order    :fact-binding :?o :constraints []}
                              {:type Customer :fact-binding :?c :constraints ['(= id (:customer-id ?o))]}
                              {:type Shipment :fact-binding :?s :constraints ['(= order-id (:id ?o))]}]
                       '(do nil))]
    :facts-fn #(let [nc (int (* N 0.3)) no (int (* N 0.3)) ns- (int (* N 0.4))]
                 (into []
                       (concat
                        (mapv (fn [i] (->Customer i :gold "US" 800)) (range nc))
                        (mapv (fn [i] (->Order i i 200.0 "electronics" :high)) (range no))
                        (mapv (fn [i] (->Shipment i (mod i no) "WH1" :pending)) (range ns-)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "15" :label "two parallel hash joins — 100k total facts"
    :productions [(mkr "r1" [{:type Order    :fact-binding :?o :constraints []}
                               {:type Customer :fact-binding :?c :constraints ['(= id (:customer-id ?o))]}]
                        '(do nil))
                  (mkr "r2" [{:type Shipment :fact-binding :?s :constraints []}
                               {:type Order    :fact-binding :?o :constraints ['(= id (:order-id ?s))]}]
                        '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :gold "US" 700)) (range 20000))
                      (mapv (fn [i] (->Order i (mod i 20000) 150.0 "books" :medium)) (range 40000))
                      (mapv (fn [i] (->Shipment i (mod i 40000) "WH2" :shipped)) (range 40000))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "16" :label "hash join + alpha filter on both sides"
    :productions [(mkr "r" [{:type Order    :fact-binding :?o :constraints ['(= category "electronics")]}
                              {:type Customer :fact-binding :?c :constraints ['(= id (:customer-id ?o))
                                                                               '(= tier :gold)]}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i (if (< (mod i 4) 1) :gold :silver) "US" 700)) (range (/ N 2)))
                      (mapv (fn [i] (->Order i (mod i (/ N 2)) 100.0
                                             (if (zero? (mod i 2)) "electronics" "books") :high))
                            (range (/ N 2)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "17" :label "acc/count per group — 1k groups × ~99 items"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints []}
                              {:accumulator '(acc/count)
                               :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                               :result-binding :?cnt}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :gold "US" 700)) (range 1000))
                      (mapv (fn [i] (->Order i (mod i 1000) 50.0 "books" :low)) (range (- N 1000)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "18" :label "acc/sum per group — 1k groups × ~99 items"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints []}
                              {:accumulator '(acc/sum :amount)
                               :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                               :result-binding :?total}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :silver "EU" 650)) (range 1000))
                      (mapv (fn [i] (->Order i (mod i 1000) (* 10.0 (mod i 100)) "clothing" :medium))
                            (range (- N 1000)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "19" :label "acc/all per group — 5k groups × ~19 items"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints ['(= tier :gold)]}
                              {:accumulator '(acc/all)
                               :from {:type Tag :constraints ['(= entity-id (:id ?c))]}
                               :result-binding :?tags}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :gold "US" 800)) (range 5000))
                      (mapv (fn [i] (->Tag (mod i 5000) (str "t" (mod i 10)))) (range (- N 5000)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "20" :label "acc/min + acc/max per group — 2k groups × ~49 items"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints []}
                              {:accumulator '(acc/min :amount)
                               :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                               :result-binding :?lo}
                              {:accumulator '(acc/max :amount)
                               :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                               :result-binding :?hi}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :gold "US" 700)) (range 2000))
                      (mapv (fn [i] (->Order i (mod i 2000) (* 5.0 (mod i 200)) "books" :low))
                            (range (- N 2000)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   ;;; ─── Section 3: Negation ─────────────────────────────────────────────────

   {:id "21" :label "negation — 100k orders, no discounts (all fire)"
    :productions [(mkr "r" [{:type Order :fact-binding :?o :constraints []}
                              [:not {:type Discount :constraints ['(= order-id (:id ?o))]}]]
                       '(do nil))]
    :facts-fn #(mapv (fn [i] (->Order i i 100.0 "books" :low)) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "22" :label "negation — 50% blocked by discounts"
    :productions [(mkr "r" [{:type Order :fact-binding :?o :constraints []}
                              [:not {:type Discount :constraints ['(= order-id (:id ?o))]}]]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Order i i 50.0 "electronics" :high)) (range N))
                      (mapv (fn [i] (->Discount i 0.1 "promo")) (range (/ N 2)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "23" :label "negation + alpha filter — 33% type, 10% flagged"
    :productions [(mkr "r" [{:type Order :constraints ['(= category "electronics")] :fact-binding :?o}
                              [:not {:type Flag :constraints ['(= entity-id (:id ?o)) '(= kind :fraud)]}]]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Order i (mod i 1000) 200.0
                                             (rand-nth ["electronics" "books" "clothing"]) :high))
                            (range N))
                      (mapv (fn [i] (->Flag (mod i N) :fraud)) (range (/ N 10)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "24" :label "double negation — 100k orders, 20k flags"
    :productions [(mkr "r" [{:type Order :fact-binding :?o :constraints []}
                              [:not {:type Discount :constraints ['(= order-id (:id ?o))]}]
                              [:not {:type Flag     :constraints ['(= entity-id (:id ?o))]}]]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Order i i 100.0 "books" :low)) (range N))
                      (mapv (fn [i] (->Flag (mod i N) :review)) (range (/ N 5)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "25" :label "negation retract trigger — insert then retract discounts"
    :productions [(mkr "r" [{:type Order :fact-binding :?o :constraints []}
                              [:not {:type Discount :constraints ['(= order-id (:id ?o))]}]]
                       '(do nil))]
    :facts-fn #(hash-map
                :orders (mapv (fn [i] (->Order i i 100.0 "electronics" :high)) (range (/ N 2)))
                :discs  (mapv (fn [i] (->Discount i 0.15 "sale")) (range (/ N 2))))
    :run-fn (fn [s {:keys [orders discs]}]
              (let [s2 (r/insert-all s (concat orders discs))
                    s3 (apply r/retract s2 discs)]
                (r/fire-rules s3)))}

   {:id "26" :label "negation — 10k orders × 9 discounts each (all blocked)"
    :productions [(mkr "r" [{:type Order :fact-binding :?o :constraints []}
                              [:not {:type Discount :constraints ['(= order-id (:id ?o))]}]]
                       '(do nil))]
    :facts-fn #(let [no (/ N 10)]
                 (into []
                       (concat
                        (mapv (fn [i] (->Order i i 100.0 "books" :low)) (range no))
                        (mapv (fn [i] (->Discount (mod i no) 0.05 "clearance")) (range (* no 9))))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "27" :label "three negation rules — 100k facts, 5k flags"
    :productions [(mkr "r1" [{:type Order :constraints ['(= category "electronics")] :fact-binding :?o}
                               [:not {:type Flag :constraints ['(= entity-id (:id ?o)) '(= kind :fraud)]}]]
                        '(do nil))
                  (mkr "r2" [{:type Order :constraints ['(= category "books")] :fact-binding :?o}
                               [:not {:type Flag :constraints ['(= entity-id (:id ?o)) '(= kind :spam)]}]]
                        '(do nil))
                  (mkr "r3" [{:type Order :constraints ['(= category "clothing")] :fact-binding :?o}
                               [:not {:type Flag :constraints ['(= entity-id (:id ?o)) '(= kind :review)]}]]
                        '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Order i (mod i 1000) 100.0
                                             (rand-nth ["electronics" "books" "clothing"]) :medium))
                            (range N))
                      (mapv (fn [i] (->Flag (mod i N) (rand-nth [:fraud :spam :review]))) (range 5000))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "28" :label "join + negation — gold customers, undiscounted orders"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints ['(= tier :gold)]}
                              {:type Order    :fact-binding :?o :constraints ['(= customer-id (:id ?c))]}
                              [:not {:type Discount :constraints ['(= order-id (:id ?o))]}]]
                       '(do nil))]
    :facts-fn #(let [nc (max 1 (int (/ N 20))) nd nc no (- N (* 2 nc))]
                 (into []
                       (concat
                        (mapv (fn [i] (->Customer i :gold "US" 800)) (range nc))
                        (mapv (fn [i] (->Order i (mod i nc) 200.0 "electronics" :high)) (range no))
                        (mapv (fn [i] (->Discount (mod i no) 0.2 "loyalty")) (range nd)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   ;;; ─── Section 4: Truth Maintenance ───────────────────────────────────────

   {:id "29" :label "logical insert — 50k trigger, 50k inert"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(> value 0.8)]}]
                       '(r/insert! (->Alert (:id ?m) :critical (:id ?m) "high")))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Metric i "cpu" 0.9 "pct")) (range (/ N 2)))
                      (mapv (fn [i] (->Metric (+ i (/ N 2)) "cpu" 0.1 "pct")) (range (/ N 2)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "30" :label "logical retract cascade — insert 50k then retract source"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(> value 0.8)]}]
                       '(r/insert! (->Alert (:id ?m) :warning (:id ?m) "elevated")))]
    :facts-fn #(mapv (fn [i] (->Metric i "mem" 0.9 "pct")) (range (/ N 2)))
    :run-fn (fn [s facts]
              (apply r/retract (-> s (r/insert-all facts) r/fire-rules) facts))}

   {:id "31" :label "two-level logical chain — 30k seed facts"
    :productions [(mkr "r1" [{:type Metric :fact-binding :?m :constraints ['(> value 0.7)]}]
                        '(r/insert! (->Flag (:id ?m) :elevated)))
                  (mkr "r2" [{:type Flag :fact-binding :?f :constraints ['(= kind :elevated)]}]
                        '(r/insert! (->Alert (:entity-id ?f) :medium (:entity-id ?f) "elevated")))]
    :facts-fn #(mapv (fn [i] (->Metric i "cpu" 0.9 "pct")) (range (int (* N 0.3))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "32" :label "logical insert + query result — 50k critical alerts"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(> value 0.9)]}]
                       '(r/insert! (->Alert (:id ?m) :critical (:id ?m) "critical")))
                  (mkq "q" #{} [{:type Alert :fact-binding :?a :constraints ['(= severity :critical)]}])]
    :facts-fn #(mapv (fn [i] (->Metric i "cpu" (if (< i (/ N 2)) 0.95 0.5) "pct")) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules (r/query "q")))}

   {:id "33" :label "unconditional insert baseline — 50k activations"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(> value 0.8)]}]
                       '(r/insert-unconditional! (->Flag (:id ?m) :flagged)))]
    :facts-fn #(mapv (fn [i] (->Metric i "cpu" 0.9 "pct")) (range (/ N 2)))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "34" :label "logical full cycle — insert→fire→retract→re-insert→fire"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(> value 0.8)]}]
                       '(r/insert! (->Alert (:id ?m) :info (:id ?m) "normal")))]
    :facts-fn #(mapv (fn [i] (->Metric i "disk" 0.9 "pct")) (range (int (* N 0.25))))
    :run-fn (fn [s facts]
              (let [s1 (-> s (r/insert-all facts) r/fire-rules)
                    s2 (apply r/retract s1 facts)]
                (-> s2 (r/insert-all facts) r/fire-rules)))}

   {:id "35" :label "truth maintenance convergence — cpu+mem two-step"
    :productions [(mkr "r1" [{:type Metric :fact-binding :?m :constraints ['(= name "cpu") '(> value 0.8)]}]
                        '(r/insert! (->Flag (:id ?m) :cpu-high)))
                  (mkr "r2" [{:type Flag   :fact-binding :?f :constraints ['(= kind :cpu-high)]}
                               {:type Metric :fact-binding :?mm
                                :constraints ['(= name "mem") '(> value 0.8) '(= id (:entity-id ?f))]}]
                        '(r/insert! (->Alert (:entity-id ?f) :high (:entity-id ?f) "cpu+mem")))]
    :facts-fn #(let [n2 (/ N 4)]
                 (into []
                       (concat
                        (mapv (fn [i] (->Metric i "cpu" 0.9 "pct")) (range n2))
                        (mapv (fn [i] (->Metric i "mem" 0.9 "pct")) (range n2)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   ;;; ─── Section 5: Rule Chains & Deep Inference ─────────────────────────────

   {:id "36" :label "5-level rule chain — 20k seed facts"
    :productions [(mkr "p1" [{:type Order :constraints ['(= priority :high)] :fact-binding :?o}]
                       '(r/insert! (->Flag (:id ?o) :s1)))
                  (mkr "p2" [{:type Flag :constraints ['(= kind :s1)] :fact-binding :?f}]
                       '(r/insert! (->Flag (:entity-id ?f) :s2)))
                  (mkr "p3" [{:type Flag :constraints ['(= kind :s2)] :fact-binding :?f}]
                       '(r/insert! (->Flag (:entity-id ?f) :s3)))
                  (mkr "p4" [{:type Flag :constraints ['(= kind :s3)] :fact-binding :?f}]
                       '(r/insert! (->Flag (:entity-id ?f) :s4)))
                  (mkr "p5" [{:type Flag :constraints ['(= kind :s4)] :fact-binding :?f}]
                       '(r/insert! (->Alert (:entity-id ?f) :low (:entity-id ?f) "end")))]
    :facts-fn #(mapv (fn [i] (->Order i (mod i 100) 100.0 "books" :high)) (range (/ N 5)))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "37" :label "10-level rule chain — 10k seed facts"
    :productions (let [seed (mkr "p-seed"
                                 [{:type Order :constraints ['(= priority :high)] :fact-binding :?o}]
                                 '(r/insert! (->Flag (:id ?o) :s0)))
                       steps (vec (for [n (range 9)]
                                    (mkr (str "p" n)
                                         [{:type Flag :constraints [(list '= 'kind (keyword (str "s" n)))]
                                           :fact-binding :?f}]
                                         (list 'r/insert!
                                               (list '->Flag
                                                     '(:entity-id ?f)
                                                     (keyword (str "s" (inc n))))))))
                       term (mkr "p-term"
                                 [{:type Flag :constraints ['(= kind :s9)] :fact-binding :?f}]
                                 '(r/insert! (->Alert (:entity-id ?f) :info (:entity-id ?f) "deep")))]
                   (into [seed term] steps))
    :facts-fn #(mapv (fn [i] (->Order i (mod i 100) 50.0 "books" :high)) (range (/ N 10)))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "38" :label "fan-out rule — 20k activations × 5 inserts each"
    :productions [(mkr "r" [{:type Order :constraints ['(= priority :high)] :fact-binding :?o}]
                       '(do
                          (r/insert! (->Flag (:id ?o) :a))
                          (r/insert! (->Flag (:id ?o) :b))
                          (r/insert! (->Flag (:id ?o) :c))
                          (r/insert! (->Flag (:id ?o) :d))
                          (r/insert! (->Flag (:id ?o) :e))))]
    :facts-fn #(mapv (fn [i] (->Order i (mod i 100) 100.0 "electronics" :high)) (range (/ N 5)))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "39" :label "three parallel 2-step chains — 100k total facts"
    :productions [(mkr "c1a" [{:type Order  :constraints ['(= priority :high)] :fact-binding :?o}]
                        '(r/insert! (->Flag (:id ?o) :chain-orders)))
                  (mkr "c1b" [{:type Flag :constraints ['(= kind :chain-orders)] :fact-binding :?f}] '(do nil))
                  (mkr "c2a" [{:type Metric :constraints ['(= name "cpu")] :fact-binding :?m}]
                        '(r/insert! (->Flag (:id ?m) :chain-metrics)))
                  (mkr "c2b" [{:type Flag :constraints ['(= kind :chain-metrics)] :fact-binding :?f}] '(do nil))
                  (mkr "c3a" [{:type Event :constraints ['(= type "click")] :fact-binding :?e}]
                        '(r/insert! (->Flag (:id ?e) :chain-events)))
                  (mkr "c3b" [{:type Flag :constraints ['(= kind :chain-events)] :fact-binding :?f}] '(do nil))]
    :facts-fn #(let [n3 (int (/ N 3))]
                 (into []
                       (concat
                        (mapv (fn [i] (->Order  i (mod i 100) 50.0 "books" :high)) (range n3))
                        (mapv (fn [i] (->Metric i "cpu" 0.5 "pct")) (range n3))
                        (mapv (fn [i] (->Event  i "click" (mod i 5000) i)) (range (- N (* 2 n3)))))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "40" :label "activation queue — 100k activations, trivial RHS"
    :productions [(mkr "r" [{:type Order :fact-binding :?o :constraints ['(> amount 0)]}] '(do nil))]
    :facts-fn #(mapv (fn [i] (->Order i (mod i 1000) 100.0 "books" :low)) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "41" :label "fire-rules + query — 10k results from 100k facts"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(> value 0.9)]}]
                       '(r/insert! (->Alert (:id ?m) :high (:id ?m) "very high")))
                  (mkq "q" #{} [{:type Alert :fact-binding :?a :constraints ['(= severity :high)]}])]
    :facts-fn #(mapv (fn [i] (->Metric i "gpu" (if (< i (/ N 10)) 0.95 0.3) "pct")) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules (r/query "q")))}

   {:id "42" :label "incremental batches — 10 × N/10 inserts with fire each"
    :productions [(mkr "r" [{:type Order :constraints ['(= category "electronics")] :fact-binding :?o}] '(do nil))]
    :facts-fn #(mapv (fn [b]
                       (mapv (fn [i] (->Order (+ (* b (/ N 10)) i) (mod i 1000) 100.0 "electronics" :high))
                             (range (/ N 10))))
                     (range 10))
    :run-fn (fn [s batches]
              (reduce (fn [sess batch] (-> sess (r/insert-all batch) r/fire-rules)) s batches))}

   {:id "43" :label "replace-facts — retract N/2 old, insert N/2 new"
    :productions [(mkr "r" [{:type Metric :fact-binding :?m :constraints ['(> value 0.5)]}] '(do nil))]
    :facts-fn #(hash-map
                :old (mapv (fn [i] (->Metric i "cpu" 0.8 "pct")) (range (/ N 2)))
                :new (mapv (fn [i] (->Metric i "cpu" 0.2 "pct")) (range (/ N 2))))
    :run-fn (fn [s {:keys [old new]}]
              (let [s1 (-> s (r/insert-all old) r/fire-rules)
                    s2 (apply r/retract s1 old)]
                (-> s2 (r/insert-all new) r/fire-rules)))}

   ;;; ─── Section 6: Mixed / Stress ───────────────────────────────────────────

   {:id "44" :label "order pipeline — flag+count+negation, 3 rules, 100k"
    :productions [(mkr "r1" [{:type Order :constraints ['(> amount 500) '(= priority :high)] :fact-binding :?o}]
                        '(r/insert! (->Flag (:id ?o) :high-value)))
                  (mkr "r2" [{:type Customer :fact-binding :?c :constraints []}
                               {:accumulator '(acc/count)
                                :from {:type Flag :constraints ['(= entity-id (:id ?c)) '(= kind :high-value)]}
                                :result-binding :?cnt}]
                        '(do nil))
                  (mkr "r3" [{:type Order :fact-binding :?o :constraints ['(= priority :high)]}
                               [:not {:type Shipment :constraints ['(= order-id (:id ?o))]}]]
                        '(r/insert! (->Alert (:id ?o) :warning (:customer-id ?o) "no shipment")))]
    :facts-fn #(let [nc (max 1 (int (/ N 20)))]
                 (into []
                       (concat
                        (mapv (fn [i] (->Customer i :gold "US" 750)) (range nc))
                        (mapv (fn [i] (->Order i (mod i nc) (if (zero? (mod i 3)) 600.0 100.0) "electronics" :high))
                              (range (int (* N 0.8))))
                        (mapv (fn [i] (->Shipment i i "WH1" :pending)) (range (int (* N 0.15)))))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "45" :label "triple accumulator — min/max/sum, 2k groups × 49 items"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints []}
                              {:accumulator '(acc/min :amount)
                               :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                               :result-binding :?lo}
                              {:accumulator '(acc/max :amount)
                               :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                               :result-binding :?hi}
                              {:accumulator '(acc/sum :amount)
                               :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                               :result-binding :?tot}]
                       '(do nil))]
    :facts-fn #(into []
                     (concat
                      (mapv (fn [i] (->Customer i :silver "EU" 600)) (range 2000))
                      (mapv (fn [i] (->Order i (mod i 2000) (* 1.0 (mod i 500)) "books" :low))
                            (range (- N 2000)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "46" :label "expression join — cross-fact predicate (ExpressionJoinNode)"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints []}
                              {:type Order :fact-binding :?o
                               :constraints ['(= customer-id (:id ?c))
                                             '(> amount (* 0.1 (:credit-score ?c)))]}]
                       '(do nil))]
    :facts-fn #(let [nc (max 1 (int (/ N 20)))]
                 (into []
                       (concat
                        (mapv (fn [i] (->Customer i :gold "US" (+ 500 (mod i 300)))) (range nc))
                        (mapv (fn [i] (->Order i (mod i nc) (+ 30.0 (mod i 200)) "electronics" :high))
                              (range (- N nc))))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "47" :label "join + accum + negation combined — 100k facts"
    :productions [(mkr "r" [{:type Customer :fact-binding :?c :constraints ['(= tier :gold)]}
                              {:accumulator '(acc/count)
                               :from {:type Order :constraints ['(= customer-id (:id ?c)) '(> amount 100)]}
                               :result-binding :?cnt}
                              [:not {:type Flag :constraints ['(= entity-id (:id ?c)) '(= kind :suspended)]}]]
                       '(do nil))]
    :facts-fn #(let [nc 3000]
                 (into []
                       (concat
                        (mapv (fn [i] (->Customer i :gold "US" 800)) (range nc))
                        (mapv (fn [i] (->Order i (mod i nc) (if (zero? (mod i 3)) 200.0 50.0) "books" :high))
                              (range (- N (* 2 nc))))
                        (mapv (fn [i] (->Flag (mod i nc) :suspended)) (range nc)))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "48" :label "acc/all — collect 100k facts into single accumulation"
    :productions [(mkr "r" [{:accumulator '(acc/all)
                               :from {:type Order :constraints ['(= priority :high)]}
                               :result-binding :?all}]
                       '(do nil))]
    :facts-fn #(mapv (fn [i] (->Order i (mod i 1000) 100.0 "books" :high)) (range N))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules))}

   {:id "49" :label "retract-refire — N/2 insert, N/4 retract, refire"
    :productions [(mkr "r" [{:type Order :constraints ['(= category "electronics")] :fact-binding :?o}]
                       '(r/insert! (->Flag (:id ?o) :processed)))]
    :facts-fn (fn []
                (let [all  (mapv (fn [i] (->Order i (mod i 1000) 100.0
                                                   (if (zero? (mod i 2)) "electronics" "books") :high))
                                 (range (/ N 2)))
                      elec (filterv (fn [o] (= "electronics" (:category o))) all)]
                  {:all all :elec elec}))
    :run-fn (fn [s {:keys [all elec]}]
              (let [s1 (-> s (r/insert-all all) r/fire-rules)
                    s2 (apply r/retract s1 elec)]
                (r/fire-rules s2)))}

   {:id "50" :label "full stress — 6 rules, 4 fact types, 100k facts, query"
    :productions [(mkr "r1" [{:type Metric :constraints ['(= name "cpu") '(> value 0.9)] :fact-binding :?m}]
                        '(r/insert! (->Alert (:id ?m) :critical (:id ?m) "cpu spike")))
                  (mkr "r2" [{:type Customer :fact-binding :?c :constraints ['(= tier :gold)]}
                               {:type Order :fact-binding :?o :constraints ['(= customer-id (:id ?c)) '(> amount 300)]}]
                        '(r/insert! (->Flag (:id ?o) :vip-order)))
                  (mkr "r3" [{:type Customer :fact-binding :?c :constraints []}
                               {:accumulator '(acc/count)
                                :from {:type Order :constraints ['(= customer-id (:id ?c))]}
                                :result-binding :?n}]
                        '(do nil))
                  (mkr "r4" [{:type Order :fact-binding :?o :constraints ['(= priority :high)]}
                               [:not {:type Shipment :constraints ['(= order-id (:id ?o))]}]]
                        '(r/insert! (->Alert (:id ?o) :warning (:customer-id ?o) "unshipped")))
                  (mkr "r5" [{:type Flag :constraints ['(= kind :vip-order)] :fact-binding :?f}]
                        '(r/insert! (->Event (:entity-id ?f) "vip-processed" (:entity-id ?f) 0)))
                  (mkr "r6" [{:type Event :constraints ['(= type "vip-processed")] :fact-binding :?e}] '(do nil))
                  (mkq "q"  #{} [{:type Alert :fact-binding :?a :constraints ['(= severity :critical)]}])]
    :facts-fn #(let [nc (max 1 (int (/ N 20)))]
                 (into []
                       (concat
                        (mapv (fn [i] (->Customer i :gold "US" 800)) (range nc))
                        (mapv (fn [i] (->Order i (mod i nc) (if (< (mod i 5) 2) 400.0 100.0) "electronics" :high))
                              (range (int (* N 0.6))))
                        (mapv (fn [i] (->Metric i "cpu" (if (< i nc) 0.95 0.4) "pct")) (range (int (* N 0.2))))
                        (mapv (fn [i] (->Shipment i (mod i (int (* N 0.6))) "WH1" :shipped)) (range (int (* N 0.15)))))))
    :run-fn (fn [s facts] (-> s (r/insert-all facts) r/fire-rules (r/query "q")))}
   ])

;;; ── Entry point ─────────────────────────────────────────────────────────────

(let [_ (println)
      _ (println "================================================================================")
      _ (println "  Clara Rules Benchmark Suite")
      _ (println (format "  N=%-6d  ITERS=%d  Clojure %s" N ITERS (clojure-version)))
      _ (println "================================================================================")
      _ (println (format "  %-6s %-64s %s" "ID" "Scenario" "mean / min / max (ms)"))
      _ (println (str "  " (apply str (repeat 80 "-"))))
      results (keep run-scenario scenarios)]
  (println)
  (println (format "  Completed %d / %d scenarios." (count results) (count scenarios)))
  (System/exit 0))

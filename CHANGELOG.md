This is a history of changes to clara-rules.

For the upstream changelog (versions 0.24.0 and earlier), see the
[oracle-samples/clara-rules](https://github.com/oracle-samples/clara-rules) repository.

# 0.25.0-SNAPSHOT

Fork-specific changes (based on upstream 0.24.0):

### Performance Optimizations

#### Alpha Network

* Add `FusedAlphaNode` for batched alpha dispatch — when a fact type has multiple alpha nodes, fuse them into a single per-fact-then-per-node pass with batch dispatch to beta children
* Add `DiscriminationNode` alpha discrimination tree for hash-based dispatch on equality constraints
* Guard `AlphaNode` listener calls with `null-listener?` to skip lazy seq allocation and protocol dispatch when no listeners are attached

#### Beta Network (Join Nodes)

* Add `when-let` guards on `HashJoinNode` and `ExpressionJoinNode` to skip cross-product when the opposing side (tokens or elements) is empty
* Add beta sub-indexing for `ExpressionJoinNode`: compile-time equality extraction with runtime hash index to reduce cross-product from O(T×E) to O(T+E)
* Fix `conj` argument order in `HashJoinNode`/`ExpressionJoinNode` `left-activate` and `left-retract` to iterate the smaller fact-bindings map
* Use `MapEntry` instead of `PersistentVector` for `[fact node-id]` match pairs (~32 bytes vs ~96 bytes on JVM)
* Specialize `select-keys` extraction in `propagate-items-to-nodes` for 1-2 key cases using map literals
* Hoist `match-pair` to element-level `:let` binding in all cross-product loops (`HashJoinNode` ×4, `ExpressionJoinNode` ×8 paths), eliminating one `MapEntry` allocation per token per element
* Add single-element fast path in `right-activate` and single-token fast path in `left-activate`/`left-retract`/`right-retract` — uses `mapv`/`keep` instead of `eager-for` for the dominant 1-fact-insert case
* Replace `platform/eager-for` with `mapv` in `RootJoinNode` `right-activate`/`right-retract` (no cross-product needed)
* Add single-item fast path in `propagate-items-to-nodes`: skip `group-by-seq` (LinkedHashMap + JavaEqualityWrapper) for single-item dispatch
* Add sub-indexing to `NegationWithJoinFilterNode`: compile-time equality extraction with `element-key-fn`/`token-key-fn`, reducing the per-token element scan from O(T×E) to O(T+E); single-element fast path avoids `group-by` allocation
* **Phase 3.1 — PHREAK-inspired demand-pull token generation**: `RootJoinNode` defers token creation when all children are demand-pull eligible `HashJoinNode`s with empty alpha-memory; `HashJoinNode.right-activate` generates tokens on-demand by pulling elements from the parent `RootJoinNode`'s alpha-memory. Avoids O(|A|) token allocations when condition A has many matching facts but condition B has none yet. Compiler computes `:left-parent-id` for eligible `HashJoinNode`s.
* **Phase 3.2 — Full PHREAK delta propagation**: Implements PHREAK-style lazy evaluation as an opt-in path (`:use-phreak true` in `fire-rules` options). Changes are queued as ordered delta operations (`TransientDeltaMemory`) and evaluated top-down in a pull phase (`run-pull-phase!`) instead of cascading eagerly through the network. Key components: `IDeltaMemory` protocol with `add-element-op!`/`add-token-op!` preserving insertion order (critical for correctness of retract-then-insert vs insert-then-retract sequences); `DeltaTransport` queues into delta memory instead of cascading; `IDeltaEvaluate.evaluate-delta` per node type; `compute-beta-topo-order` (DFS pre-order) ensures parents evaluate before children; integrated with `opts-fixture` to run all tests under PHREAK alongside standard and cancelling paths.

#### Accumulators

* Optimize `drop-one-of` for vectors using indexed lookup + `subvec` splicing instead of lazy seq scan/rebuild
* Add 3-arity `do-accumulate` to avoid `Accumulator` record allocation via `assoc` when providing initial value
* Optimize merge in `send-accumulated` and `retract-accumulated` with `cond` + `conj`/`assoc`

#### Fire-Rules Loop

* Replace `eager-for` macro with `doseq` + transient vector construction, eliminating lazy seq allocation at ~26 call sites
* Replace atoms with volatiles in single-threaded `fire-rules*` paths
* Add `RuleContext` defrecord to replace per-activation map allocation
* Replace dynamic `*rule-context*` binding with a volatile on `*current-session*` to eliminate `pushThreadBindings`/`popThreadBindings` per activation
* Hoist per-activation volatiles before the fire-rules loop
* Optimize `flush-updates` to eagerly group consecutive same-type updates instead of lazy `partition-by` + `mapcat`

#### Working Memory

* Replace `get-in`/`assoc-in` with nested `get`/`assoc` in accumulator memory paths to eliminate key-path vector allocation
* Replace `OrderedUpdateCache` atom with mutable `ArrayList` (CLJ) / mutable field (CLJS) to remove CAS overhead during single-threaded fire-rules
* Replace lazy seqs with eager variants in `filter-accum-facts`, `pre-reduce`, `matches-some-facts?`, and `DelegatingListener` transitions
* Clean up `to-persistent!` and CLJS `get-activations` lazy seqs

### Build & Infrastructure

* Migrate build system from Leiningen to Clojure CLI (deps.edn + tools.build)
* Target Java 21+
* Upgrade dependencies: Clojure 1.12.4, ClojureScript 1.12.101, Prismatic Schema 1.4.1
* Replace Puppeteer-based CLJS tests with Node.js target
* Fix javac compilation warnings (use `--release` flag instead of `-source`/`-target`)
* Fix CLJS compilation warning in `testing_utils.cljc` (missing `cljs.test` require)
* Fix clj-kondo errors for `defrule`/`defquery` docstrings in hooks
* Remove Oracle-specific files (CONTRIBUTING.md, SECURITY.md, RELEASE.md)
* Convert README to AsciiDoc

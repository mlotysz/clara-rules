(ns clara.tools.inspect
  "Tooling to inspect a rule session. The two major methods here are:

   * inspect, which returns a data structure describing the session that can be used by tooling.
   * explain-activations, which uses inspect and prints a human-readable description covering
     why each rule activation or query match occurred."
  (:require #?(:clj [clara.rules.engine :as eng])
    #?(:cljs [clara.rules.engine :as eng :refer [RootJoinNode
                                                 HashJoinNode
                                                 ExpressionJoinNode
                                                 NegationNode
                                                 NegationWithJoinFilterNode
                                                 ProductionNode]])
    [clara.rules.schema :as schema]
    [clara.rules.memory :as mem]
    [clara.rules.platform :as platform]
    [clara.tools.internal.inspect :as i]
    [clojure.set :as set]
    #?(:cljs [goog.string :as gstr])
    #?(:clj
    [clojure.main :refer [demunge]])
    [schema.core :as s]
    [clojure.string :as str])
  #?(:clj
    (:import [clara.rules.engine
              ProductionNode
              RootJoinNode
              HashJoinNode
              ExpressionJoinNode
              NegationNode
              NegationWithJoinFilterNode
              ISystemFact])))

(s/defschema ConditionMatch
  "A structure associating a condition with the facts that matched them.  The fields are:
   :fact - A fact propagated from this condition in a rule or query.  For non-accumulator conditions,
           this will be the fact matched by the condition.  For accumulator conditions, it will be the result
           of the accumulation.  So, for example, if we have a condition like

           [?cold <- Cold]

           a ConditionMatch for this condition will have a Cold fact in its :fact field.  If we have a condition like

           [?min-cold <- (acc/min :temperature) :from [Cold]]
     
           the value of :fact will be the minimum temperature returned by the accumulator.

    :condition - A structure representing this condition.  This is the same structure used inside the structures defining
                 rules and queries.

    :facts-accumulated (nullable) : When the condition is an accumulator condition, this will contain the individual facts over 
                                    which the accumulator ran.  For example, in the case above with the condition

                                    [?min-cold <- (acc/min :temperature) :from [Cold]]
                                    
                                    this will contain the individual Cold facts over which we accumulated, while the :fact field
                                    will contain the result of the accumulation."
  {:fact s/Any
   :condition schema/Condition
   (s/optional-key :facts-accumulated) [s/Any]})

;; A structured explanation of why a rule or query matched.
;; This is derived from the Rete-style tokens, but this token
;; is designed to propagate all context needed to easily inspect
;; the state of rules.  
(s/defrecord Explanation [matches :- [ConditionMatch]
                          bindings :- {s/Keyword s/Any}]) ; Bound variables

;; Schema of an inspected rule session.
(def InspectionSchema
  {:rule-matches {schema/Rule [Explanation]}
   :query-matches {schema/Query [Explanation]}
   :condition-matches {schema/Condition [s/Any]}
   :insertions {schema/Rule [{:explanation Explanation :fact s/Any}]}})

(defn- get-condition-matches
  "Returns facts matching each condition"
  [nodes memory]
  (let [node-class->node-type (fn [node]
                                (get {ExpressionJoinNode :join
                                      HashJoinNode :join
                                      RootJoinNode :join
                                      NegationNode :negation
                                      NegationWithJoinFilterNode :negation} (type node)))

        join-node-ids (for [beta-node nodes
                            :let [node-type (node-class->node-type beta-node)]
                            ;; Unsupported and irrelevant node types will have a node-type of nil
                            ;; since the map in node-class->node-type won't contain an entry
                            ;; for them, so this check will remove them.
                            :when (contains? #{:join :negation}
                                             node-type)]
                        [(:id beta-node) (:condition beta-node) node-type])]
    (reduce
     (fn [matches [node-id condition node-type]]
       (update-in matches
                  (condp = node-type

                    :join
                    [condition]

                    ;; Negation nodes store the fact that they are a negation
                    ;; in their :node-type and strip the information out of the
                    ;; :condition field.  We reconstruct the negation boolean condition
                    ;; that is contained in rule and query data structures created by defrule
                    ;; and that conforms to the Condition schema.
                    :negation
                    [[:not condition]])
                  concat (map :fact (mem/get-elements-all memory {:id node-id}))))
     {}
     join-node-ids)))

(defn- to-explanations
  "Helper function to convert tokens to explanation records."
  [session tokens]
  (let [memory (-> session eng/components :memory)
        id-to-node (get-in (eng/components session) [:rulebase :id-to-node])]

    (for [{:keys [matches bindings] :as token} tokens]
      (->Explanation
       ;; Convert matches to explanation structure.
       (for [[fact node-id] matches
             :let [node (id-to-node node-id)
                   condition (if (:accum-condition node)

                               {:accumulator (get-in node [:accum-condition :accumulator])
                                :from {:type (get-in node [:accum-condition :from :type])
                                       :constraints (or (seq (get-in node [:accum-condition :from :original-constraints]))
                                                        (get-in node [:accum-condition :from :constraints]))}}

                               {:type (:type (:condition node))
                                :constraints (or (seq (:original-constraints (:condition node)))
                                                 (:constraints (:condition node)))})]]
         (if (:accum-condition node)
           {:fact fact
            :condition condition
            :facts-accumulated (eng/token->matching-elements node memory token)}
           {:fact fact
            :condition condition}))

       ;; Remove generated bindings from user-facing explanation.
       (into {} (remove (fn [[k v]]
                          #?(:clj (.startsWith (name k) "?__gen__"))
                          #?(:cljs (gstr/startsWith (name k) "?__gen__")))
                        bindings))))))

(defn ^:private gen-all-rule-matches
  [session]
  (when-let [activation-info (i/get-activation-info session)]
    (let [grouped-info (group-by #(-> % :activation :node) activation-info)]
      (into {}
            (map (fn [[k v]]
                   [(:production k)
                    (to-explanations session (map #(-> % :activation :token) v))]))
            grouped-info))))
                    
(defn ^:private gen-fact->explanations
  [session]

  (let [{:keys [memory rulebase]} (eng/components session)
        {:keys [productions production-nodes query-nodes]} rulebase
        rule-to-rule-node (into {} (for [rule-node production-nodes]
                                           [(:production rule-node) rule-node]))]
    (apply merge-with into
           (for [[rule rule-node] rule-to-rule-node
                 token (keys (mem/get-insertions-all memory rule-node))
                 insertion-group (mem/get-insertions memory rule-node token)
                 insertion insertion-group]
             {insertion [{:rule rule
                          :explanation (first (to-explanations session [token]))}]}))))

(def ^{:doc "Return a new session on which information will be gathered for optional inspection keys.
             This can significantly increase memory consumption since retracted facts
             cannot be garbage collected as normally."}
  with-full-logging i/with-activation-listening)

(def ^{:doc "Return a new session without information gathering on this session for optional inspection keys.
             This new session will not retain references to any such information previously gathered."}
  without-full-logging i/without-activation-listening)
        
(s/defn inspect
  " Returns a representation of the given rule session useful to understand the
   state of the underlying rules.

   The returned structure always includes the following keys:

   * :rule-matches -- a map of rule structures to their matching explanations.
     Note that this only includes rule matches with corresponding logical 
     insertions after the rules finished firing.
   * :query-matches -- a map of query structures to their matching explanations.
   * :condition-matches -- a map of conditions pulled from each rule to facts they match.
   * :insertions -- a map of rules to a sequence of {:explanation E, :fact F} records
     to allow inspection of why a given fact was inserted.
   * :fact->explanations -- a map of facts inserted to a sequence 
     of maps of the form {:rule rule-structure :explanation explanation}, 
     where each such map justifies a single insertion of the fact.

   And additionally includes the following keys for operations 
   performed after a with-full-logging call on the session:
   
   * :unfiltered-rule-matches: A map of rule structures to their matching explanations.
     This includes all rule activations, regardless of whether they led to insertions or if
     they were ultimately retracted.  This should be considered low-level information primarily
     useful for debugging purposes rather than application control logic, although legitimate use-cases
     for the latter do exist if care is taken.  Patterns of insertion and retraction prior to returning to
     the caller are internal implementation details of Clara unless explicitly controlled by the user.

   Users may inspect the entire structure for troubleshooting or explore it
   for specific cases. For instance, the following code snippet could look
   at all matches for some example rule:

   (defrule example-rule ... )

   ...

   (get-in (inspect example-session) [:rule-matches example-rule])

   ...

   The above segment will return matches for the rule in question."
  [session] :- InspectionSchema
  (let [{:keys [memory rulebase]} (eng/components session)
        {:keys [productions production-nodes query-nodes id-to-node]} rulebase

        ;; Map of queries to their nodes in the network.
        query-to-nodes (into {} (for [[query-name query-node] query-nodes]
                                  [(:query query-node) query-node]))

        ;; Map of rules to their nodes in the network.
        rule-to-nodes (into {} (for [rule-node production-nodes]
                                 [(:production rule-node) rule-node]))

        base-info {:rule-matches (into {}
                                       (for [[rule rule-node] rule-to-nodes]
                                         [rule (to-explanations session
                                                                (keys (mem/get-insertions-all memory rule-node)))]))

                   :query-matches (into {}
                                        (for [[query query-node] query-to-nodes]
                                          [query (to-explanations session
                                                                  (mem/get-tokens-all memory query-node))]))

                   :condition-matches (get-condition-matches (vals id-to-node) memory)

                   :insertions (into {}
                                     (for [[rule rule-node] rule-to-nodes]
                                       [rule
                                        (for [token (keys (mem/get-insertions-all memory rule-node))
                                              insertion-group (get (mem/get-insertions-all memory rule-node) token)
                                              insertion insertion-group]
                                          {:explanation (first (to-explanations session [token])) :fact insertion})]))

                   :fact->explanations (gen-fact->explanations session)}]

    (if-let [unfiltered-rule-matches (gen-all-rule-matches session)]
      (assoc base-info :unfiltered-rule-matches unfiltered-rule-matches)
      base-info)))

(defn- explain-activation
  "Prints a human-readable explanation of the facts and conditions that created the Rete token."
  ([explanation] (explain-activation explanation ""))
  ([explanation prefix]
     (doseq [{:keys [fact condition]} (:matches explanation)]
       (if (:from condition)
         ;; Explain why the accumulator matched.
         (let [{:keys [accumulator from]} condition]
           (println prefix fact)
           (println prefix "  accumulated with" accumulator)
           (println prefix "  from" (:type from))
           (println prefix "  where" (:constraints from)))

         ;; Explain why a condition matched.
         (let [{:keys [type constraints]} condition]
           (println prefix fact)
           (println prefix "  is a" type)
           (println prefix "  where" constraints))))))

(defn explain-activations
  "Prints a human-friendly explanation of why rules and queries matched in the given session.
  A caller my optionally pass a :rule-filter-fn, which is a predicate

  (clara.tools.inspect/explain-activations session
         :rule-filter-fn (fn [rule] (re-find my-rule-regex (:name rule))))"

  [session & {:keys [rule-filter-fn] :as options}]
  (let [filter-fn (or rule-filter-fn (constantly true))]

    (doseq [[rule explanations] (:rule-matches (inspect session))
            :when (filter-fn rule)
            :when (seq explanations)]
      (println "rule"  (or (:name rule) (str "<" (:lhs rule) ">")))
      (println "  executed")
      (println "   " (:rhs rule))
      (doseq [explanation explanations]
        (println "  with bindings")
        (println "    " (:bindings explanation))
        (println "  because")
        (explain-activation explanation "    "))
      (println))

    (doseq [[rule explanations] (:query-matches (inspect session))
            :when (filter-fn rule)
            :when (seq explanations)]
      (println "query"  (or (:name rule) (str "<" (:lhs rule) ">")))
      (doseq [explanation explanations]
        (println "  with bindings")
        (println "    " (:bindings explanation))
        (println "  qualified because")
        (explain-activation explanation "    "))
      (println))))

(defn- gen-binding?
  "Returns true if the keyword is a generated binding (internal to Clara)."
  [k]
  #?(:clj (.startsWith (name k) "?__gen__")
     :cljs (gstr/startsWith (name k) "?__gen__")))

(defn- dissoc-gen-bindings
  "Removes generated bindings from a bindings map."
  [bindings]
  (into {} (remove (fn [[k _]] (gen-binding? k))) bindings))

(defn- system-fact?
  "Returns true if the given fact is an internal system fact (e.g. NegationResult)."
  [fact]
  #?(:clj (instance? ISystemFact fact)
     :cljs (isa? (type fact) :clara.rules.engine/system-type)))

(defn get-root-facts
  "Returns the set of root (user-inserted) facts in the session. Root facts are facts that
   were inserted directly by the user, as opposed to facts derived by rule activations.
   Uses identity-based deduplication to distinguish identical-value facts inserted separately."
  [session]
  (let [{:keys [memory rulebase]} (eng/components session)
        {:keys [production-nodes id-to-node]} rulebase

        ;; Collect all facts from alpha memory, using identity wrappers for dedup.
        ;; Alpha memory stores Element records with :fact and :bindings fields.
        all-fact-wrappers (into #{}
                                (comp (mapcat (fn [[_node-id bindings-map]]
                                                (mapcat identity (vals bindings-map))))
                                      (map :fact)
                                      (remove system-fact?)
                                      (map platform/fact-id-wrap))
                                (:alpha-memory memory))

        ;; Collect all facts inserted by production nodes (rule-derived facts).
        rule-derived-wrappers (into #{}
                                    (comp (mapcat (fn [prod-node]
                                                    (let [insertions-map (mem/get-insertions-all memory prod-node)]
                                                      (mapcat (fn [[_token insertion-groups]]
                                                                (mapcat identity insertion-groups))
                                                              insertions-map))))
                                          (remove system-fact?)
                                          (map platform/fact-id-wrap))
                                    production-nodes)]
    (into []
          (comp (map platform/fact-id-unwrap))
          (set/difference all-fact-wrappers rule-derived-wrappers))))

(def FactsInspectionSchema
  "Schema for the return value of inspect-facts."
  {:rules {s/Int schema/Rule}
   :facts [{:fact s/Any
            (s/optional-key :rule-id) s/Int
            (s/optional-key :bindings) {s/Keyword s/Any}
            :fact-types [s/Any]}]})

(defn inspect-facts
  "Returns a data structure describing all facts in the session and their provenance.

   The returned map has:
   * :rules — a map of integer IDs to production (rule) structures for rules that derived facts.
   * :facts — a vector of maps, each with:
     - :fact — the fact value
     - :fact-types — vector of types this fact matches in the alpha network
     - :rule-id (optional) — ID into the :rules map, present if this fact was derived by a rule
     - :bindings (optional) — the bindings that caused the rule to fire, present if rule-derived

   The fact-type-fn and ancestors-fn are extracted from the session's get-alphas-fn metadata
   when available, falling back to `type` and `ancestors` respectively."
  [session]
  (let [{:keys [memory rulebase get-alphas-fn]} (eng/components session)
        {:keys [production-nodes]} rulebase

        ;; Extract type/ancestors functions from get-alphas-fn metadata, with fallbacks.
        alphas-meta (meta get-alphas-fn)
        fact-type-fn (or (:fact-type-fn alphas-meta) type)
        ancestors-fn (or (:ancestors-fn alphas-meta) ancestors)

        ;; Build a map from identity-wrapped fact -> {:rule production, :bindings bindings}
        fact-wrapper->provenance
        (reduce (fn [acc prod-node]
                  (let [insertions-map (mem/get-insertions-all memory prod-node)]
                    (reduce-kv
                     (fn [acc token insertion-groups]
                       (let [bindings (dissoc-gen-bindings (:bindings token))]
                         (reduce (fn [acc insertion-group]
                                   (reduce (fn [acc fact]
                                             (if (system-fact? fact)
                                               acc
                                               (assoc acc (platform/fact-id-wrap fact)
                                                      {:rule (:production prod-node)
                                                       :bindings bindings})))
                                           acc
                                           insertion-group))
                                 acc
                                 insertion-groups)))
                     acc
                     insertions-map)))
                {}
                production-nodes)

        ;; Collect all non-system facts from alpha memory using identity dedup.
        ;; Alpha memory stores Element records; extract the :fact field.
        all-facts (into []
                        (comp (mapcat (fn [[_node-id bindings-map]]
                                        (mapcat identity (vals bindings-map))))
                              (map :fact)
                              (remove system-fact?)
                              (distinct))
                        (:alpha-memory memory))

        ;; Assign integer IDs to distinct rules that derived facts.
        rules-set (into #{} (comp (map val) (map :rule)) fact-wrapper->provenance)
        rule->id (into {} (map-indexed (fn [idx rule] [rule idx])) rules-set)
        id->rule (into {} (map (fn [[rule id]] [id rule])) rule->id)]

    {:rules id->rule
     :facts (mapv (fn [fact]
                    (let [wrapper (platform/fact-id-wrap fact)
                          provenance (get fact-wrapper->provenance wrapper)
                          ft (fact-type-fn fact)
                          fact-types (into [ft] (ancestors-fn ft))
                          base {:fact fact :fact-types fact-types}]
                      (if provenance
                        (assoc base
                               :rule-id (get rule->id (:rule provenance))
                               :bindings (:bindings provenance))
                        base)))
                  all-facts)}))

(let [inverted-type-lookup (zipmap (vals eng/node-type->abbreviated-type)
                                   (keys eng/node-type->abbreviated-type))]
  (defn node-fn-name->production-name
    "A helper function for retrieving the name or names of rules that a generated function belongs to.

     'session' - a LocalSession from which a function was retrieved
     'node-fn' - supports the following types:
        1. String   - expected to be in the format '<namespace>/<Node abbreviation>_<NodeId>_<Function abbreviation>'.
                      Expected use-case for string would be in the event that a user copy pasted this function identifier
                      from an external tool, ex. a jvm profiler
        2. Symbol   - expected to be in the format '<namespace>/<Node abbreviation>_<NodeId>_<Function abbreviation>.
                      Has the same use-case as string, just adds flexibility to the type.
        3. Function - expected to be the actual function from the Session
                      This covers a use-case where the user can capture the function being used and programmatically
                      trace it back to the rules being executed."
    [session node-fn]
    (let [fn-name-str (cond
                        (string? node-fn)
                        node-fn

                        (fn? node-fn)
                        #?(:clj (str node-fn)
                           :cljs (.-name node-fn) )

                        (symbol? node-fn)
                        (str node-fn)

                        :else
                        (throw (ex-info "Unsupported type for 'node-fn-name->production-name'"
                                        {:type (type node-fn)
                                         :supported-types ["string" "symbol" "fn"]})))
          fn-name-str (-> fn-name-str demunge (str/split #"/") last)

          simple-fn-name #?(:clj
                            (-> (or (re-find #"(.+)--\d+" fn-name-str) ;; anonymous function
                                    (re-find #"(.+)" fn-name-str)) ;; regular function
                                last)
                            :cljs
                            fn-name-str)

          [node-abr node-id _] (str/split simple-fn-name #"-")]
      ;; used as a sanity check that the fn provided is in the form expected, ie. <NodeAbr>-<NodeId>-<FnType>
      (if (contains? inverted-type-lookup node-abr)
        (if-let [node (-> (eng/components session)
                          :rulebase
                          :id-to-node
                          (get #?(:clj (Long/valueOf ^String node-id)
                                  :cljs (js/parseInt node-id))))]
          (if (= ProductionNode (type node))
            [(-> node :production :name)]
            (if-let [production-names (seq (eng/node-rule-names (some-fn :production :query) node))]
              production-names
              ;; This should be un-reachable but i am leaving it here in the event that the rulebase is somehow corrupted
              (throw (ex-info "Unable to determine suitable name from node"
                              {:node node}))))
          (throw (ex-info "Node-id not found in rulebase"
                          {:node-id node-id
                           :simple-name simple-fn-name})))
        (throw (ex-info "Unable to determine node from function"
                        {:name node-fn
                         :simple-name simple-fn-name}))))))

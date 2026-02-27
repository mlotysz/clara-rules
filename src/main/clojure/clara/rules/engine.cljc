(ns clara.rules.engine
  "This namespace is for internal use and may move in the future. Most users should use only the clara.rules namespace."
  (:require [clojure.core.reducers :as r]
            [schema.core :as s]
            [clojure.string :as string]
            [clara.rules.memory :as mem]
            [clara.rules.listener :as l]
            #?(:clj [clara.rules.platform :as platform]
               :cljs [clara.rules.platform :as platform :include-macros true])
            [clara.rules.update-cache.core :as uc]
            [clara.rules.delta-memory :as dm]
            #?(:clj [clara.rules.update-cache.cancelling :as ca])))

;; The accumulator is a Rete extension to run an accumulation (such as sum, average, or similar operation)
;; over a collection of values passing through the Rete network. This object defines the behavior
;; of an accumulator. See the AccumulateNode for the actual node implementation in the network.
(defrecord Accumulator [initial-value retract-fn reduce-fn combine-fn convert-return-fn])

;; A Rete-style token, which contains two items:
;; * matches, a vector of [fact, node-id] tuples for the facts and corresponding nodes they matched.
;; NOTE:  It is important that this remains an indexed vector for memory optimizations as well as
;;        for correct conj behavior for new elements i.e. added to the end.
;; * bindings, a map of keyword-to-values for bound variables.
(defrecord Token [matches bindings])

;; A working memory element, containing a single fact and its corresponding bound variables.
(defrecord Element [fact bindings])

;; An activation for the given production and token.
(defrecord Activation [node token])

;; Token with no bindings, used as the root of beta nodes.
(def empty-token (->Token [] {}))

;; Creates a [fact node-id] match pair for Token :matches vectors.
;; On the JVM, MapEntry is ~32 bytes vs ~96 bytes for a 2-element PersistentVector,
;; while supporting the same destructuring, nth, first/second, seq, and equality.
(defn- match-pair [fact node-id]
  #?(:clj (clojure.lang.MapEntry/create fact node-id)
     :cljs [fact node-id]))

;; Record indicating the negation existing in the working memory.
;;
;; Determining if an object is an instance of a class is a primitive
;; JVM operation and is much more efficient than determining
;; if that object descends from a particular object through
;; Clojure's hierarchy as determined by the isa? function.
;; See Issue 239 for more details.
#?(:clj
    (do
      ;; A marker interface to identify internal facts.
      (definterface ISystemFact)
      (defrecord NegationResult [gen-rule-name ancestor-bindings]
        ISystemFact))

    :cljs
    (do
      (defrecord NegationResult [gen-rule-name ancestor-bindings])
      ;; Make NegationResult a "system type" so that NegationResult
      ;; facts are special-cased when matching productions. This serves
      ;; the same purpose as implementing the ISystemFact Java interface
      ;; on the Clojure version of NegationResult.
      ;; ClojureScript does not have definterface; if we experience performance
      ;; problems in ClojureScript similar to those on the JVM that are
      ;; described in issue 239 we can investigate a similar strategy in JavaScript.
      (derive NegationResult ::system-type)))

;; Schema for the structure returned by the components
;; function on the session protocol.
;; This is simply a comment rather than first-class schema
;; for now since it's unused for validation and created
;; undesired warnings as described at https://groups.google.com/forum/#!topic/prismatic-plumbing/o65PfJ4CUkI
(comment

  (def session-components-schema
    {:rulebase s/Any
     :memory s/Any
     :transport s/Any
     :listeners [s/Any]
     :get-alphas-fn s/Any}))

;; Returns a new session with the additional facts inserted.
(defprotocol ISession

  ;; Inserts facts.
  (insert [session facts])

  ;; Retracts facts.
  (retract [session facts])

  ;; Fires pending rules and returns a new session where they are in a fired state.
  ;;
  ;; Note that clara.rules/fire-rules, the public API for these methods, will handle
  ;; calling the two-arg fire-rules with an empty map itself, but we add handle it in the fire-rules implementation
  ;; as well in case anyone is directly calling the fire-rules protocol function or interface method on the LocalSession.
  ;; The two-argument version of fire-rules was added for issue 249.
  (fire-rules [session] [session opts])

  ;; Runs a query agains thte session.
  (query [session query params])

  ;; Returns the components of a session as defined in the session-components-schema
  (components [session]))

;; Left activation protocol for various types of beta nodes.
(defprotocol ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener])
  (left-retract [node join-bindings tokens memory transport listener])
  (description [node])
  (get-join-keys [node]))

;; Right activation protocol to insert new facts, connecting alpha nodes
;; and beta nodes.
(defprotocol IRightActivate
  (right-activate [node join-bindings elements memory transport listener])
  (right-retract [node join-bindings elements memory transport listener]))

;; Specialized right activation interface for accumulator nodes,
;; where the caller has the option of pre-reducing items
;; to reduce the data sent to the node. This would be useful
;; if the caller is not in the same memory space as the accumulator node itself.
(defprotocol IAccumRightActivate
  ;; Pre-reduces elements, returning a map of bindings to reduced elements.
  (pre-reduce [node elements])

  ;; Right-activate the node with items reduced in the above pre-reduce step.
  (right-activate-reduced [node join-bindings reduced  memory transport listener]))

(defprotocol IAccumInspect
  "This protocol is expected to be implemented on accumulator nodes in the rules network.
   It is not expected that users will implement this protocol, and most likely will not call
   the protocol function directly."
  (token->matching-elements [node memory token]
    "Takes a token that was previously propagated from the node, 
     or a token that is a descendant of such a token, and returns the facts in elements 
     matching the token propagated from the node.  During rules firing
     accumulators only propagate bindings created and the result binding
     downstream rather than all facts that were accumulated over, but there
     are use-cases in session inspection where we want to retrieve the individual facts.
    
     Example: [?min-temp <- (acc/min :temperature) :from [Temperature (= temperature ?loc)]]
              [?windspeed <- [WindSpeed (= location ?loc)]]
     
     Given a token propagated from the node for the WindSpeed condition 
     we could retrieve the Temperature facts from the matching location."))

;; The transport protocol for sending and retracting items between nodes.
(defprotocol ITransport
  (send-elements [transport memory listener nodes elements])
  (send-tokens [transport memory listener nodes tokens])
  (retract-elements [transport memory listener nodes elements])
  (retract-tokens [transport memory listener nodes tokens]))

(defn- propagate-items-to-nodes [transport memory listener nodes items propagate-fn]
  (doseq [node nodes
          :let [join-keys (get-join-keys node)]]

    (if (pos? (count join-keys))

      ;; Group by the join keys for the activation.
      ;; Specialize extraction for common key counts to avoid select-keys overhead.
      (let [n (count join-keys)
            extract-fn (case n
                         1 (let [k (first join-keys)]
                             #(let [b (:bindings %)] {k (b k)}))
                         2 (let [[k1 k2] (seq join-keys)]
                             #(let [b (:bindings %)] {k1 (b k1) k2 (b k2)}))
                         #(select-keys (:bindings %) join-keys))]
        (if (next items)
          (doseq [[join-bindings item-group] (platform/group-by-seq extract-fn items)]
            (propagate-fn node
                          join-bindings
                          item-group
                          memory
                          transport
                          listener))
          (when-let [item (first items)]
            (propagate-fn node (extract-fn item) items memory transport listener))))

      ;; The node has no join keys, so just send everything at once
      ;; (if there is something to send.)
      (when (seq items)
        (propagate-fn node
                      {}
                      items
                      memory
                      transport
                      listener)))))

;; Simple, in-memory transport.
(deftype LocalTransport []
  ITransport
  (send-elements [transport memory listener nodes elements]
    (propagate-items-to-nodes transport memory listener nodes elements right-activate))

  (send-tokens [transport memory listener nodes tokens]
    (propagate-items-to-nodes transport memory listener nodes tokens left-activate))

  (retract-elements [transport memory listener nodes elements]
    (propagate-items-to-nodes transport memory listener nodes elements right-retract))

  (retract-tokens [transport memory listener nodes tokens]
    (propagate-items-to-nodes transport memory listener nodes tokens left-retract)))

;; PHREAK: protocol for pull-phase evaluation of queued delta updates.
(defprotocol IDeltaEvaluate
  "Called during the PHREAK pull phase to drain queued element/token deltas for a node,
   update working memory, compute downstream token deltas, and queue them to children
   via the DeltaTransport rather than cascading immediately."
  (evaluate-delta [node memory delta-memory transport listener]))

;; PHREAK: transport that queues deltas instead of cascading immediately.
;; All send-*/retract-* calls record the delta in delta-memory and mark the
;; target nodes dirty; actual evaluation happens in the pull phase.
(deftype DeltaTransport [delta-memory]
  ITransport
  (send-elements [transport memory listener nodes elements]
    (propagate-items-to-nodes transport memory listener nodes elements
                              (fn [node join-bindings elems _mem _t _l]
                                (dm/add-element-op! delta-memory (:id node) join-bindings elems true)
                                (dm/mark-dirty! delta-memory (:id node)))))

  (send-tokens [transport memory listener nodes tokens]
    (propagate-items-to-nodes transport memory listener nodes tokens
                              (fn [node join-bindings toks _mem _t _l]
                                (dm/add-token-op! delta-memory (:id node) join-bindings toks true)
                                (dm/mark-dirty! delta-memory (:id node)))))

  (retract-elements [transport memory listener nodes elements]
    (propagate-items-to-nodes transport memory listener nodes elements
                              (fn [node join-bindings elems _mem _t _l]
                                (dm/add-element-op! delta-memory (:id node) join-bindings elems false)
                                (dm/mark-dirty! delta-memory (:id node)))))

  (retract-tokens [transport memory listener nodes tokens]
    (propagate-items-to-nodes transport memory listener nodes tokens
                              (fn [node join-bindings toks _mem _t _l]
                                (dm/add-token-op! delta-memory (:id node) join-bindings toks false)
                                (dm/mark-dirty! delta-memory (:id node))))))

;; Protocol for activation of Rete alpha nodes.
(defprotocol IAlphaActivate
  (alpha-activate [node facts memory transport listener])
  (alpha-retract [node facts memory transport listener]))

;; Protocol for getting the type (e.g. :production and :query) and name of a
;; terminal node.
(defprotocol ITerminalNode
  (terminal-node-type [this]))

;; Protocol for getting a node's condition expression.
(defprotocol IConditionNode
  (get-condition-description [this]))

(defn get-terminal-node-types
  [node]
  (->> node
       (tree-seq (comp seq :children) :children)
       (keep #(when (satisfies? ITerminalNode %)
                (terminal-node-type %)))
       (into (sorted-set))))

(defn get-conditions-and-rule-names
  "Returns a map from conditions to sets of rules."
  ([node]
   (if-let [condition (when (satisfies? IConditionNode node)
                           (get-condition-description node))]
     {condition (get-terminal-node-types node)}
     (->> node
         :children
         (map get-conditions-and-rule-names)
         (reduce (partial merge-with into) {})))))

;; Active session during rule execution.
(def ^:dynamic *current-session* nil)

;; When bound, this map is merged into every fire-rules opts call.
;; Intended for test fixtures that need to exercise alternate engine paths
;; (e.g., {:use-phreak true}) without modifying call sites.
(def ^:dynamic *additional-fire-rules-opts* {})

;; Note that this can hold facts directly retracted and facts logically retracted
;; as a result of an external retraction or insertion.
;; The value is expected to be an atom holding such facts.
(def ^:dynamic *pending-external-retractions* nil)

;; The per-activation rule context, stored as a volatile on *current-session*
;; rather than a dynamic binding to avoid pushThreadBindings/popThreadBindings overhead.

(defn ^:private external-retract-loop
  "Retract all facts, then group and retract all facts that must be logically retracted because of these
   retractions, and so forth, until logical consistency is reached.  When an external retraction causes multiple
  facts of the same type to be retracted in the same iteration of the loop this improves efficiency since they can be grouped.
  For example, if we have a rule that matches on FactA and inserts FactB, and then a later rule that accumulates on FactB,
  if we have multiple FactA external retractions it is more efficient to logically retract all the FactB instances at once to minimize the  number of times we must re-accumulate on FactB.
  This is similar to the function of the pending-updates in the fire-rules* loop."
  [get-alphas-fn memory transport listener]
  (loop []
    (let [retractions (deref *pending-external-retractions*)
          ;; We have already obtained a direct reference to the facts to be
          ;; retracted in this iteration of the loop outside the cache.  Now reset
          ;; the cache.  The retractions we execute may cause new retractions to be queued
          ;; up, in which case the loop will execute again.
          _ (vreset! *pending-external-retractions* [])]
      (doseq [[alpha-roots fact-group] (get-alphas-fn retractions)
              root alpha-roots]
        (alpha-retract root fact-group memory transport listener))
      (when (-> *pending-external-retractions* deref not-empty)
        (recur)))))

(defn- flush-updates
  "Flush all pending updates in the current session. Returns true if there were
   some items to flush, false otherwise"
  [current-session]
  (letfn [(flush-all [current-session flushed-items?]
            (let [{:keys [_rulebase transient-memory transport _insertions get-alphas-fn listener]} current-session
                  pending-updates (-> current-session :pending-updates uc/get-updates-and-reset!)]

              (if (empty? pending-updates)
                flushed-items?
                (do
                  (doseq [[update-type facts] pending-updates
                          [alpha-roots fact-group] (get-alphas-fn facts)
                          root alpha-roots]

                    (if (= :insert update-type)
                      (alpha-activate root fact-group transient-memory transport listener)
                      (alpha-retract root fact-group transient-memory transport listener)))

                  ;; There may be new pending updates due to the flush just
                  ;; made.  So keep flushing until there are none left.  Items
                  ;; were flushed though, so flush-items? is now true.
                  (flush-all current-session true)))))]

    (flush-all current-session false)))

(defn insert-facts!
  "Place facts in a stateful cache to be inserted into the session
  immediately after the RHS of a rule fires."
  [facts unconditional]
  (let [rule-context @(:rule-context *current-session*)]
    (if unconditional
      (vswap! (:batched-unconditional-insertions rule-context) into facts)
      (vswap! (:batched-logical-insertions rule-context) into facts))))

(defn rhs-retract-facts!
  "Place all facts retracted in the RHS in a buffer to be retracted after
   the eval'ed RHS function completes."
  [facts]
  (vswap! (:batched-rhs-retractions @(:rule-context *current-session*)) into facts))

(defn ^:private flush-rhs-retractions!
  "Retract all facts retracted in the RHS after the eval'ed RHS function completes.
  This should only be used for facts explicitly retracted in a RHS.
  It should not be used for retractions that occur as part of automatic truth maintenance."
  [facts]
  (let [{:keys [_rulebase transient-memory transport insertions get-alphas-fn listener]} *current-session*
        {:keys [node token]} @(:rule-context *current-session*)]
    ;; Update the count so the rule engine will know when we have normalized.
    (vswap! insertions + (count facts))

    (when-not (l/null-listener? listener)
      (l/retract-facts! listener node token facts))

    (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (alpha-retract root fact-group transient-memory transport listener))))

(defn ^:private flush-insertions!
  "Perform the actual fact insertion, optionally making them unconditional.  This should only
   be called once per rule activation for logical insertions."
  [facts unconditional]
  (let [{:keys [_rulebase transient-memory _transport insertions _get-alphas-fn listener]} *current-session*
        {:keys [node token]} @(:rule-context *current-session*)]

    ;; Update the insertion count.
    (vswap! insertions + (count facts))

    ;; Track this insertion in our transient memory so logical retractions will remove it.
    (if unconditional
      (l/insert-facts! listener node token facts)
      (do
        (mem/add-insertions! transient-memory node token facts)
        (l/insert-facts-logical! listener node token facts)))

    (-> *current-session* :pending-updates (uc/add-insertions! facts))))

(defn retract-facts!
  "Perform the fact retraction."
  [facts]
  (-> *current-session* :pending-updates (uc/add-retractions! facts)))

;; Record for the production node in the Rete network.
(defrecord ProductionNode [id production rhs]
  ILeftActivate
  (left-activate [node _join-bindings tokens memory _transport listener]

    ;; Provide listeners information on all left-activate calls,
    ;; but we don't store these tokens in the beta-memory since the production-memory
    ;; and activation-memory collectively contain all information that ProductionNode
    ;; needs.  See https://github.com/cerner/clara-rules/issues/386
    (l/left-activate! listener node tokens)

    ;; Fire the rule if it's not a no-loop rule, or if the rule is not
    ;; active in the current context.
    (when (or (not (get-in production [:props :no-loop]))
              (not (= production (some-> *current-session* :rule-context deref :node :production))))

      (let [activations (platform/eager-for [token tokens]
                                            (->Activation node token))]

        (l/add-activations! listener node activations)

        ;; The production matched, so add the tokens to the activation list.
        (mem/add-activations! memory production activations))))

  (left-retract [node join-bindings tokens memory _transport listener]

    ;; Provide listeners information on all left-retract calls for passivity,
    ;; but we don't store these tokens in the beta-memory since the production-memory
    ;; and activation-memory collectively contain all information that ProductionNode
    ;; needs.  See https://github.com/cerner/clara-rules/issues/386
    (l/left-retract! listener node tokens)

    ;; Remove pending activations triggered by the retracted tokens.
    (let [activations (platform/eager-for [token tokens]
                                          (->Activation node token))

          ;; We attempt to remove a pending activation for all tokens retracted, but our expectation
          ;; is that each token may remove a pending activation
          ;; or logical insertions from a previous rule activation but not both.
          ;; We first attempt to use each token to remove a pending activation but keep track of which
          ;; tokens were not used to remove an activation.
          [removed-activations unremoved-activations]
          (mem/remove-activations! memory production activations)

          _ (l/remove-activations! listener node removed-activations)

          unremoved-tokens (mapv :token unremoved-activations)

          ;; Now use each token that was not used to remove a pending activation to remove
          ;; the logical insertions from a previous activation if the truth maintenance system
          ;; has a matching previous activation.
          token-insertion-map (mem/remove-insertions! memory node unremoved-tokens)]

      (when-let [insertions (not-empty (into [] cat (vals token-insertion-map)))]
        ;; If there is current session with rules firing, add these items to the queue
        ;; to be retracted so they occur in the same order as facts being inserted.
        (cond

          ;; Both logical retractions resulting from rule network activity and manual RHS retractions
          ;; expect *current-session* to be bound since both happen in the context of a fire-rules call.
          *current-session*
          ;; Retract facts that have become untrue, unless they became untrue
          ;; because of an activation of the current rule that is :no-loop
          (when (or (not (get-in production [:props :no-loop]))
                    (not (= production (some-> *current-session* :rule-context deref :node :production))))
            ;; Notify the listener of logical retractions.
            ;; Note that this notification happens immediately, while the
            ;; alpha-retract notification on matching alpha nodes will happen when the
            ;; retraction is actually removed from the buffer and executed in the rules network.
            (doseq [[token token-insertions] token-insertion-map]
              (l/retract-facts-logical! listener node token token-insertions))
            (retract-facts! insertions))

          ;; Any session implementation is required to bind this during external retractions and insertions.
          *pending-external-retractions*
          (do
            (doseq [[token token-insertions] token-insertion-map]
              (l/retract-facts-logical! listener node token token-insertions))
            (vswap! *pending-external-retractions* into insertions))

          :else
          (throw (ex-info (str "Attempting to retract from a ProductionNode when neither *current-session* nor "
                               "*pending-external-retractions* is bound is illegal.")
                          {:node node
                           :join-bindings join-bindings
                           :tokens tokens}))))))

  (get-join-keys [_node] [])

  (description [_node] "ProductionNode")

  ITerminalNode
  (terminal-node-type [_this] [:production (:name production)]))

;; The QueryNode is a terminal node that stores the
;; state that can be queried by a rule user.
(defrecord QueryNode [id query param-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory _transport listener]
    (l/left-activate! listener node tokens)
    (mem/add-tokens! memory node join-bindings tokens))

  (left-retract [node join-bindings tokens memory _transport listener]
    (l/left-retract! listener node tokens)
    (mem/remove-tokens! memory node join-bindings tokens))

  (get-join-keys [_node] param-keys)

  (description [_node] (str "QueryNode -- " query))

  ITerminalNode
  (terminal-node-type [_this] [:query (:name query)]))


(defn node-rule-names
  [child-type node]
  (->> node
       (tree-seq (comp seq :children) :children)
       (keep child-type)
       (map :name)
       (distinct)
       (sort)))

(defn- list-of-names
  "Returns formatted string with correctly pluralized header and
  list of names. Returns nil if no such node is found."
  [singular plural prefix names]
  (let [msg-for-unnamed (str "  An unnamed " singular ", provide names to your "
                             plural " if you want them to be identified here.")
        names-string (->> names
                          (sort)
                          (map #(if (nil? %) msg-for-unnamed %))
                          (map #(str prefix "  " %))
                          (string/join "\n"))]
    (if (pos? (count names))
      (str prefix plural ":\n" names-string "\n")
      nil)))


(defn- single-condition-message
  [condition-number [condition-definition terminals]]
  (let [productions (->> terminals
                         (filter (comp #{:production} first))
                         (map second))
        queries (->> terminals
                     (filter (comp #{:query} first))
                     (map second))
        production-section (list-of-names "rule" "rules" "   " productions)
        query-section (list-of-names "query" "queries" "   " queries)]
    (string/join
     [(str (inc condition-number) ". " condition-definition "\n")
      production-section
      query-section])))

(defn- throw-condition-exception
  "Adds a useful error message when executing a constraint node raises an exception."
  [{:keys [cause node fact env bindings] :as args}]
  (let [bindings-description (if (empty? bindings)
                               "with no bindings"
                               (str "with bindings\n  " bindings))
        facts-description (if (contains? args :fact)
                            (str "when processing fact\n " (pr-str fact))
                            "with no fact")
        message-header (string/join ["Condition exception raised.\n"
                                     (str facts-description "\n")
                                     (str bindings-description "\n")
                                     "Conditions:\n"])
        conditions-and-rules (get-conditions-and-rule-names node)
        condition-messages (->> conditions-and-rules
                                (map-indexed single-condition-message)
                                (string/join "\n"))
        message (str message-header "\n" condition-messages)]
    (throw (ex-info message
                    {:fact fact
                     :bindings bindings
                     :env env
                     :conditions-and-rules conditions-and-rules}
                    cause))))

(defn- alpha-node-matches
  [facts env activation node]
  (platform/eager-for [fact facts
                       :let [bindings (try (activation fact env)
                                           (catch #?(:clj Exception :cljs :default) e
                                               (throw-condition-exception {:cause e
                                                                           :node node
                                                                           :fact fact
                                                                           :env env})))]
                       :when bindings]           ; FIXME: add env.
                      [fact bindings]))

;; Record representing alpha nodes in the Rete network,
;; each of which evaluates a single condition and
;; propagates matches to its children.
(defrecord AlphaNode [id env children activation fact-type]

  IAlphaActivate
  (alpha-activate [node facts memory transport listener]
    (let [fact-binding-pairs (alpha-node-matches facts env activation node)]
      (when-not (l/null-listener? listener)
        (l/alpha-activate! listener node (mapv first fact-binding-pairs)))
      (send-elements
       transport
       memory
       listener
       children
       (platform/eager-for [[fact bindings] fact-binding-pairs]
                           (->Element fact bindings)))))

  (alpha-retract [node facts memory transport listener]
    (let [fact-binding-pairs (alpha-node-matches facts env activation node)]
      (when-not (l/null-listener? listener)
        (l/alpha-retract! listener node (mapv first fact-binding-pairs)))
      (retract-elements
        transport
        memory
        listener
        children
        (platform/eager-for [[fact bindings] fact-binding-pairs]
                            (->Element fact bindings))))))

;; A fused alpha node that wraps multiple AlphaNodes for the same fact type.
;; Instead of N independent alpha-activate dispatches per fact batch, a single
;; FusedAlphaNode iterates per-fact-then-per-node for better cache locality,
;; accumulates elements per original node, then batch-dispatches to beta children.
(defrecord FusedAlphaNode [id entries fact-type]
  ;; entries: vector of {:id, :env, :children, :activation, :node (original AlphaNode)}
  IAlphaActivate
  (alpha-activate [_this facts memory transport listener]
    (let [n (count entries)
          slots (object-array n)]
      ;; For each fact, try each entry's activation
      (doseq [fact facts]
        (dotimes [i n]
          (let [entry (nth entries i)
                bindings (try ((:activation entry) fact (:env entry))
                              (catch #?(:clj Exception :cljs :default) e
                                (throw-condition-exception {:cause e
                                                           :node (:node entry)
                                                           :fact fact
                                                           :env (:env entry)})))]
            (when bindings
              (let [element (->Element fact bindings)
                    existing (aget slots i)]
                (aset slots i (if existing
                                (conj existing element)
                                [element])))))))
      ;; Dispatch accumulated elements per entry
      (let [null-listener (l/null-listener? listener)]
        (dotimes [i n]
          (when-let [elements (aget slots i)]
            (let [entry (nth entries i)]
              (when-not null-listener
                (l/alpha-activate! listener (:node entry) (mapv :fact elements)))
              (send-elements transport memory listener (:children entry) elements)))))))

  (alpha-retract [_this facts memory transport listener]
    (let [n (count entries)
          slots (object-array n)]
      ;; For each fact, try each entry's activation
      (doseq [fact facts]
        (dotimes [i n]
          (let [entry (nth entries i)
                bindings (try ((:activation entry) fact (:env entry))
                              (catch #?(:clj Exception :cljs :default) e
                                (throw-condition-exception {:cause e
                                                           :node (:node entry)
                                                           :fact fact
                                                           :env (:env entry)})))]
            (when bindings
              (let [element (->Element fact bindings)
                    existing (aget slots i)]
                (aset slots i (if existing
                                (conj existing element)
                                [element])))))))
      ;; Dispatch accumulated elements per entry
      (let [null-listener (l/null-listener? listener)]
        (dotimes [i n]
          (when-let [elements (aget slots i)]
            (let [entry (nth entries i)]
              (when-not null-listener
                (l/alpha-retract! listener (:node entry) (mapv :fact elements)))
              (retract-elements transport memory listener (:children entry) elements))))))))

;; A discrimination-tree alpha node that uses hash-based lookup on a single
;; equality constraint field to skip non-matching entries. Falls back to
;; evaluating default entries (those without the discriminated constraint).
(defrecord DiscriminationNode [id accessor-kw dispatch-map default-indices entries fact-type]
  ;; accessor-kw: keyword like :temperature, used as (accessor-kw fact) to extract field value
  ;; dispatch-map: {constant-value -> int-array of entry indices}
  ;; default-indices: int-array of indices for entries WITHOUT the discriminated constraint
  ;; entries: vector of {:id :env :children :activation :node} (same as FusedAlphaNode)
  IAlphaActivate
  (alpha-activate [_this facts memory transport listener]
    (let [n (count entries)
          slots (object-array n)]
      (doseq [fact facts]
        (let [field-val (accessor-kw fact)
              matched (get dispatch-map field-val)]
          ;; Evaluate matched indices (entries with this field value)
          (when matched
            (let [len (alength ^ints matched)]
              (dotimes [j len]
                (let [i (aget ^ints matched j)
                      entry (nth entries i)
                      bindings (try ((:activation entry) fact (:env entry))
                                    (catch #?(:clj Exception :cljs :default) e
                                      (throw-condition-exception {:cause e
                                                                  :node (:node entry)
                                                                  :fact fact
                                                                  :env (:env entry)})))]
                  (when bindings
                    (let [element (->Element fact bindings)
                          existing (aget slots i)]
                      (aset slots i (if existing
                                      (conj existing element)
                                      [element]))))))))
          ;; Evaluate default indices (entries without the discriminated constraint)
          (let [dlen (alength ^ints default-indices)]
            (dotimes [j dlen]
              (let [i (aget ^ints default-indices j)
                    entry (nth entries i)
                    bindings (try ((:activation entry) fact (:env entry))
                                  (catch #?(:clj Exception :cljs :default) e
                                    (throw-condition-exception {:cause e
                                                                :node (:node entry)
                                                                :fact fact
                                                                :env (:env entry)})))]
                (when bindings
                  (let [element (->Element fact bindings)
                        existing (aget slots i)]
                    (aset slots i (if existing
                                    (conj existing element)
                                    [element])))))))))
      ;; Dispatch accumulated elements per entry
      (let [null-listener (l/null-listener? listener)]
        (dotimes [i n]
          (when-let [elements (aget slots i)]
            (let [entry (nth entries i)]
              (when-not null-listener
                (l/alpha-activate! listener (:node entry) (mapv :fact elements)))
              (send-elements transport memory listener (:children entry) elements)))))))

  (alpha-retract [_this facts memory transport listener]
    (let [n (count entries)
          slots (object-array n)]
      (doseq [fact facts]
        (let [field-val (accessor-kw fact)
              matched (get dispatch-map field-val)]
          (when matched
            (let [len (alength ^ints matched)]
              (dotimes [j len]
                (let [i (aget ^ints matched j)
                      entry (nth entries i)
                      bindings (try ((:activation entry) fact (:env entry))
                                    (catch #?(:clj Exception :cljs :default) e
                                      (throw-condition-exception {:cause e
                                                                  :node (:node entry)
                                                                  :fact fact
                                                                  :env (:env entry)})))]
                  (when bindings
                    (let [element (->Element fact bindings)
                          existing (aget slots i)]
                      (aset slots i (if existing
                                      (conj existing element)
                                      [element]))))))))
          (let [dlen (alength ^ints default-indices)]
            (dotimes [j dlen]
              (let [i (aget ^ints default-indices j)
                    entry (nth entries i)
                    bindings (try ((:activation entry) fact (:env entry))
                                  (catch #?(:clj Exception :cljs :default) e
                                    (throw-condition-exception {:cause e
                                                                :node (:node entry)
                                                                :fact fact
                                                                :env (:env entry)})))]
                (when bindings
                  (let [element (->Element fact bindings)
                        existing (aget slots i)]
                    (aset slots i (if existing
                                    (conj existing element)
                                    [element])))))))))
      (let [null-listener (l/null-listener? listener)]
        (dotimes [i n]
          (when-let [elements (aget slots i)]
            (let [entry (nth entries i)]
              (when-not null-listener
                (l/alpha-retract! listener (:node entry) (mapv :fact elements)))
              (retract-elements transport memory listener (:children entry) elements))))))))

(defn- best-discriminating-field
  "Given a sequence of [entry-index discriminators] pairs, selects the best field
   to discriminate on. Returns {:field kw :coverage n :entries [{:index :value}]}
   or nil if no field covers 2+ entries."
  [indexed-discriminators]
  (let [;; Build {field -> [{:index entry-index :value constant-value}]}
        field-groups (reduce
                      (fn [m [entry-idx discs]]
                        (reduce (fn [m {:keys [field value]}]
                                  (update m field (fnil conj []) {:index entry-idx :value value}))
                                m discs))
                      {}
                      indexed-discriminators)]
    (when (seq field-groups)
      (let [best (reduce-kv
                  (fn [best field entries]
                    (let [coverage (count entries)]
                      (if (and (>= coverage 2)
                               (or (nil? best)
                                   (> coverage (:coverage best))
                                   ;; Tiebreak: more distinct values = better hash spread
                                   (and (= coverage (:coverage best))
                                        (> (count (distinct (map :value entries)))
                                           (count (distinct (map :value (:entries best))))))))
                        {:field field :coverage coverage :entries entries}
                        best)))
                  nil
                  field-groups)]
        best))))

(defn fuse-alpha-nodes
  "Given a vector of AlphaNodes for the same fact-type, returns a single
   FusedAlphaNode or DiscriminationNode wrapping them when there are 2+ nodes.
   For single-node vectors (or empty), returns the nodes as-is.
   When discrimination metadata is available on 2+ nodes, builds a
   DiscriminationNode that uses hash-based lookup to skip non-matching entries."
  [create-id-fn alpha-nodes]
  (if (< (count alpha-nodes) 2)
    alpha-nodes
    (let [entries (mapv (fn [node]
                          {:id (:id node)
                           :env (:env node)
                           :children (:children node)
                           :activation (:activation node)
                           :node node})
                        alpha-nodes)
          ;; Collect discriminator metadata from AlphaNode records
          indexed-discs (into []
                              (keep-indexed
                               (fn [idx node]
                                 (when-let [discs (:clara.rules.compiler/discriminators (meta node))]
                                   [idx discs])))
                              alpha-nodes)]
      (if (>= (count indexed-discs) 2)
        ;; Try to build a discrimination node
        (if-let [{:keys [field _coverage entries-info]}
                 (when-let [best (best-discriminating-field indexed-discs)]
                   {:field (:field best)
                    :coverage (:coverage best)
                    :entries-info (:entries best)})]
          (let [;; Set of entry indices that have the discriminated constraint
                discriminated-indices (into #{} (map :index) entries-info)
                ;; Build dispatch-map: {value -> int-array of entry indices}
                dispatch-map (reduce
                              (fn [m {:keys [index value]}]
                                (update m value (fnil conj []) index))
                              {}
                              entries-info)
                dispatch-map (into {}
                                   (map (fn [[v indices]]
                                          [v (int-array indices)]))
                                   dispatch-map)
                ;; Default indices: entries without the discriminated constraint
                default-indices (int-array
                                 (into []
                                       (remove discriminated-indices)
                                       (range (count entries))))]
            [(->DiscriminationNode
              (create-id-fn)
              field
              dispatch-map
              default-indices
              entries
              (:fact-type (first alpha-nodes)))])
          ;; No good discriminating field found, fall back to FusedAlphaNode
          [(->FusedAlphaNode (create-id-fn) entries (:fact-type (first alpha-nodes)))])
        ;; Not enough discriminators, fall back to FusedAlphaNode
        [(->FusedAlphaNode (create-id-fn) entries (:fact-type (first alpha-nodes)))]))))

(defrecord RootJoinNode [id condition children binding-keys]
  ILeftActivate
  (left-activate [_node _join-bindings _tokens _memory _transport _listener]
    ;; This specialized root node doesn't need to deal with the
    ;; empty token, so do nothing.
    )

  (left-retract [_node _join-bindings _tokens _memory _transport _listener]
    ;; The empty token can't be retracted from the root node,
    ;; so do nothing.
    )

  (get-join-keys [_node] binding-keys)

  (description [_node] (str "RootJoinNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    (l/right-activate! listener node elements)

    ;; Add elements to the working memory to support analysis tools.
    (mem/add-elements! memory node join-bindings elements)
    ;; Optimization 3.1: If all children support demand-pull (have :left-parent-id set)
    ;; and have no alpha-elements yet, defer token creation.  Tokens will be generated
    ;; on-demand in HashJoinNode.right-activate when elements first arrive there.
    (when-not (and (every? :left-parent-id children)
                   (every? #(empty? (mem/get-elements-all memory %)) children))
      ;; Simply create tokens and send it downstream.
      (send-tokens
       transport
       memory
       listener
       children
       (mapv (fn [{:keys [fact bindings]}]
               (->Token [(match-pair fact id)] bindings))
             elements))))

  (right-retract [node join-bindings elements memory transport listener]

    (l/right-retract! listener node elements)

    ;; Remove matching elements and send the retraction downstream.
    (retract-tokens
     transport
     memory
     listener
     children
     (mapv (fn [{:keys [fact bindings]}]
             (->Token [(match-pair fact id)] bindings))
           (mem/remove-elements! memory node join-bindings elements))))

  IConditionNode
  (get-condition-description [_this]
    (let [{:keys [type constraints]} condition]
      (into [type] constraints))))

;; Record for the join node, a type of beta node in the rete network. This node performs joins
;; between left and right activations, creating new tokens when joins match and sending them to
;; its descendents.
(defrecord HashJoinNode [id condition children binding-keys left-parent-id]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (l/left-activate! listener node tokens)
    ;; Short-circuit: skip cross-product when no elements exist for these bindings.
    (when-let [elements (seq (mem/get-elements memory node join-bindings))]
      (send-tokens
       transport
       memory
       listener
       children
       (if (next tokens)
         (platform/eager-for [element elements
                              :let [fact (:fact element)
                                    fact-binding (:bindings element)
                                    mp (match-pair fact id)]
                              token tokens]
                             (->Token (conj (:matches token) mp) (conj (:bindings token) fact-binding)))
         (let [token (first tokens)]
           (mapv (fn [element]
                   (let [fact (:fact element)
                         fact-binding (:bindings element)]
                     (->Token (conj (:matches token) (match-pair fact id)) (conj (:bindings token) fact-binding))))
                 elements))))))

  (left-retract [node join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (let [removed-tokens (mem/remove-tokens! memory node join-bindings tokens)]
      ;; Short-circuit: skip cross-product when no elements exist for these bindings.
      (when-let [elements (seq (mem/get-elements memory node join-bindings))]
        (retract-tokens
         transport
         memory
         listener
         children
         (if (next removed-tokens)
           (platform/eager-for [element elements
                                :let [fact (:fact element)
                                      fact-bindings (:bindings element)
                                      mp (match-pair fact id)]
                                token removed-tokens]
                               (->Token (conj (:matches token) mp) (conj (:bindings token) fact-bindings)))
           (let [token (first removed-tokens)]
             (mapv (fn [element]
                     (let [fact (:fact element)
                           fact-bindings (:bindings element)]
                       (->Token (conj (:matches token) (match-pair fact id)) (conj (:bindings token) fact-bindings))))
                   elements)))))))

  (get-join-keys [_node] binding-keys)

  (description [_node] (str "JoinNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    (l/right-activate! listener node elements)
    ;; Optimization 3.1: demand-pull tokens from the RootJoinNode parent when no tokens
    ;; exist yet for these bindings (deferred-token case).  If left-parent-id is nil,
    ;; or tokens already exist, use the normal short-circuit path.
    (let [existing-tokens (mem/get-tokens memory node join-bindings)
          tokens (or (seq existing-tokens)
                     (when left-parent-id
                       ;; RootJoinNode stores all elements at join-bindings={}.
                       ;; Filter to those whose bindings match the current join-bindings.
                       (let [parent-elements (mem/get-elements memory {:id left-parent-id} {})
                             matched (if (empty? join-bindings)
                                       parent-elements
                                       (filter (fn [e]
                                                 (= join-bindings
                                                    (select-keys (:bindings e) binding-keys)))
                                               parent-elements))]
                         (when (seq matched)
                           (let [gen-tokens (mapv (fn [{:keys [fact bindings]}]
                                                    (->Token [(match-pair fact left-parent-id)] bindings))
                                                  matched)]
                             (mem/add-tokens! memory node join-bindings gen-tokens)
                             gen-tokens)))))]
      (when tokens
        (send-tokens
         transport
         memory
         listener
         children
         (if (next elements)
           (platform/eager-for [{:keys [fact bindings] :as _element} elements
                                :let [mp (match-pair fact id)]
                                token tokens]
                               (->Token (conj (:matches token) mp) (conj (:bindings token) bindings)))
           (let [{:keys [fact bindings]} (first elements)
                 mp (match-pair fact id)]
             (mapv (fn [token]
                     (->Token (conj (:matches token) mp) (conj (:bindings token) bindings)))
                   tokens)))))))

  (right-retract [node join-bindings elements memory transport listener]
    (l/right-retract! listener node elements)
    (let [removed-elements (mem/remove-elements! memory node join-bindings elements)]
      ;; Short-circuit: skip cross-product when no tokens exist for these bindings.
      (when-let [tokens (seq (mem/get-tokens memory node join-bindings))]
        (retract-tokens
         transport
         memory
         listener
         children
         (if (next removed-elements)
           (platform/eager-for [{:keys [fact bindings] :as _element} removed-elements
                                :let [mp (match-pair fact id)]
                                token tokens]
                               (->Token (conj (:matches token) mp) (conj (:bindings token) bindings)))
           (let [{:keys [fact bindings]} (first removed-elements)
                 mp (match-pair fact id)]
             (mapv (fn [token]
                     (->Token (conj (:matches token) mp) (conj (:bindings token) bindings)))
                   tokens)))))))

  IConditionNode
  (get-condition-description [_this]
    (let [{:keys [type constraints]} condition]
      (into [type] constraints))))

(defn- join-node-matches
  [node join-filter-fn token fact fact-bindings env]
  (let [beta-bindings (try (join-filter-fn token fact fact-bindings {})
                           (catch #?(:clj Exception :cljs :default) e
                               (throw-condition-exception {:cause e
                                                           :node node
                                                           :fact fact
                                                           :env env
                                                           :bindings (merge (:bindings token)
                                                                            fact-bindings)})))]
    beta-bindings))

(defrecord ExpressionJoinNode [id condition join-filter-fn children binding-keys
                               element-key-fn token-key-fn left-parent-id]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (l/left-activate! listener node tokens)
    ;; Short-circuit: skip cross-product when no elements exist for these bindings.
    (when-let [elements (seq (mem/get-elements memory node join-bindings))]
      (if element-key-fn
        ;; Sub-indexed path: build local element index, look up by token key
        (let [element-index (group-by #(element-key-fn (:fact %) (:bindings %)) elements)]
          (send-tokens
           transport memory listener children
           (if (next tokens)
             (platform/eager-for [token tokens
                                  :let [key (token-key-fn token)]
                                  element (get element-index key [])
                                  :let [fact (:fact element)
                                        fact-binding (:bindings element)
                                        mp (match-pair fact id)
                                        beta-bindings (if join-filter-fn
                                                        (join-node-matches node join-filter-fn token fact fact-binding {})
                                                        {})]
                                  :when (if join-filter-fn beta-bindings true)]
                                 (->Token (conj (:matches token) mp)
                                          (conj (:bindings token) fact-binding beta-bindings)))
             (let [token (first tokens)
                   key (token-key-fn token)]
               (into []
                     (keep (fn [element]
                             (let [fact (:fact element)
                                   fact-binding (:bindings element)
                                   beta-bindings (if join-filter-fn
                                                   (join-node-matches node join-filter-fn token fact fact-binding {})
                                                   {})]
                               (when (if join-filter-fn beta-bindings true)
                                 (->Token (conj (:matches token) (match-pair fact id))
                                          (conj (:bindings token) fact-binding beta-bindings))))))
                     (get element-index key []))))))
        ;; Original path: full cross-product with filter
        (send-tokens
         transport
         memory
         listener
         children
         (if (next tokens)
           (platform/eager-for [element elements
                                :let [fact (:fact element)
                                      fact-binding (:bindings element)
                                      mp (match-pair fact id)]
                                token tokens
                                :let [beta-bindings (join-node-matches node join-filter-fn token fact fact-binding {})]
                                :when beta-bindings]
                               (->Token (conj (:matches token) mp)
                                        (conj (:bindings token) fact-binding beta-bindings)))
           (let [token (first tokens)]
             (into []
                   (keep (fn [element]
                           (let [fact (:fact element)
                                 fact-binding (:bindings element)
                                 beta-bindings (join-node-matches node join-filter-fn token fact fact-binding {})]
                             (when beta-bindings
                               (->Token (conj (:matches token) (match-pair fact id))
                                        (conj (:bindings token) fact-binding beta-bindings))))))
                   elements)))))))

  (left-retract [node join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (let [removed-tokens (mem/remove-tokens! memory node join-bindings tokens)]
      ;; Short-circuit: skip cross-product when no elements exist for these bindings.
      (when-let [elements (seq (mem/get-elements memory node join-bindings))]
        (if element-key-fn
          ;; Sub-indexed path
          (let [element-index (group-by #(element-key-fn (:fact %) (:bindings %)) elements)]
            (retract-tokens
             transport memory listener children
             (if (next removed-tokens)
               (platform/eager-for [token removed-tokens
                                    :let [key (token-key-fn token)]
                                    element (get element-index key [])
                                    :let [fact (:fact element)
                                          fact-bindings (:bindings element)
                                          mp (match-pair fact id)
                                          beta-bindings (if join-filter-fn
                                                          (join-node-matches node join-filter-fn token fact fact-bindings {})
                                                          {})]
                                    :when (if join-filter-fn beta-bindings true)]
                                   (->Token (conj (:matches token) mp)
                                            (conj (:bindings token) fact-bindings beta-bindings)))
               (let [token (first removed-tokens)
                     key (token-key-fn token)]
                 (into []
                       (keep (fn [element]
                               (let [fact (:fact element)
                                     fact-bindings (:bindings element)
                                     beta-bindings (if join-filter-fn
                                                     (join-node-matches node join-filter-fn token fact fact-bindings {})
                                                     {})]
                                 (when (if join-filter-fn beta-bindings true)
                                   (->Token (conj (:matches token) (match-pair fact id))
                                            (conj (:bindings token) fact-bindings beta-bindings))))))
                       (get element-index key []))))))
          ;; Original path
          (retract-tokens
           transport
           memory
           listener
           children
           (if (next removed-tokens)
             (platform/eager-for [element elements
                                  :let [fact (:fact element)
                                        fact-bindings (:bindings element)
                                        mp (match-pair fact id)]
                                  token removed-tokens
                                  :let [beta-bindings (join-node-matches node join-filter-fn token fact fact-bindings {})]
                                  :when beta-bindings]
                                 (->Token (conj (:matches token) mp)
                                          (conj (:bindings token) fact-bindings beta-bindings)))
             (let [token (first removed-tokens)]
               (into []
                     (keep (fn [element]
                             (let [fact (:fact element)
                                   fact-bindings (:bindings element)
                                   beta-bindings (join-node-matches node join-filter-fn token fact fact-bindings {})]
                               (when beta-bindings
                                 (->Token (conj (:matches token) (match-pair fact id))
                                          (conj (:bindings token) fact-bindings beta-bindings))))))
                     elements))))))))

  (get-join-keys [_node] binding-keys)

  (description [_node] (str "JoinNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    (l/right-activate! listener node elements)
    ;; Demand-pull tokens from parent when token memory is empty (deferred-token case).
    ;; Skipped in PHREAK mode (DeltaTransport) because RootJoinNode.evaluate-delta always
    ;; sends token ops; demand-pull here would produce duplicates when those ops arrive.
    (let [existing-tokens (mem/get-tokens memory node join-bindings)
          tokens (or (seq existing-tokens)
                     (when (and left-parent-id
                                (not (instance? DeltaTransport transport)))
                       (let [parent-elements (mem/get-elements memory {:id left-parent-id} {})
                             matched (if (empty? join-bindings)
                                       parent-elements
                                       (filter (fn [e]
                                                 (= join-bindings
                                                    (select-keys (:bindings e) binding-keys)))
                                               parent-elements))]
                         (when (seq matched)
                           (let [gen-tokens (mapv (fn [{:keys [fact bindings]}]
                                                    (->Token [(match-pair fact left-parent-id)] bindings))
                                                  matched)]
                             (mem/add-tokens! memory node join-bindings gen-tokens)
                             gen-tokens)))))]
      (when tokens
        (if token-key-fn
          ;; Sub-indexed path: build local token index, look up by element key
          (send-tokens
           transport memory listener children
           (if (next elements)
             (let [token-index (group-by token-key-fn tokens)]
               (platform/eager-for [{:keys [fact bindings] :as _element} elements
                                    :let [key (element-key-fn fact bindings)
                                          mp (match-pair fact id)]
                                    token (get token-index key [])
                                    :let [beta-bindings (if join-filter-fn
                                                          (join-node-matches node join-filter-fn token fact bindings {})
                                                          {})]
                                    :when (if join-filter-fn beta-bindings true)]
                                   (->Token (conj (:matches token) mp)
                                            (conj (:bindings token) bindings beta-bindings))))
             (let [{:keys [fact bindings]} (first elements)
                   key (element-key-fn fact bindings)
                   mp (match-pair fact id)]
               (into []
                     (keep (fn [token]
                             (when (= key (token-key-fn token))
                               (let [beta-bindings (if join-filter-fn
                                                     (join-node-matches node join-filter-fn token fact bindings {})
                                                     {})]
                                 (when (if join-filter-fn beta-bindings true)
                                   (->Token (conj (:matches token) mp)
                                            (conj (:bindings token) bindings beta-bindings)))))))
                     tokens))))
          ;; Original path
          (send-tokens
           transport
           memory
           listener
           children
           (if (next elements)
             (platform/eager-for [{:keys [fact bindings] :as _element} elements
                                  :let [mp (match-pair fact id)]
                                  token tokens
                                  :let [beta-bindings (join-node-matches node join-filter-fn token fact bindings {})]
                                  :when beta-bindings]
                                 (->Token (conj (:matches token) mp)
                                          (conj (:bindings token) bindings beta-bindings)))
             (let [{:keys [fact bindings]} (first elements)
                   mp (match-pair fact id)]
               (into []
                     (keep (fn [token]
                             (let [beta-bindings (join-node-matches node join-filter-fn token fact bindings {})]
                               (when beta-bindings
                                 (->Token (conj (:matches token) mp)
                                          (conj (:bindings token) bindings beta-bindings))))))
                     tokens))))))))

  (right-retract [node join-bindings elements memory transport listener]
    (l/right-retract! listener node elements)
    (let [removed-elements (mem/remove-elements! memory node join-bindings elements)]
      ;; Short-circuit: skip cross-product when no tokens exist for these bindings.
      (when-let [tokens (seq (mem/get-tokens memory node join-bindings))]
        (if token-key-fn
          ;; Sub-indexed path
          (retract-tokens
           transport memory listener children
           (if (next removed-elements)
             (let [token-index (group-by token-key-fn tokens)]
               (platform/eager-for [{:keys [fact bindings] :as _element} removed-elements
                                    :let [key (element-key-fn fact bindings)
                                          mp (match-pair fact id)]
                                    token (get token-index key [])
                                    :let [beta-bindings (if join-filter-fn
                                                          (join-node-matches node join-filter-fn token fact bindings {})
                                                          {})]
                                    :when (if join-filter-fn beta-bindings true)]
                                   (->Token (conj (:matches token) mp)
                                            (conj (:bindings token) bindings beta-bindings))))
             (let [{:keys [fact bindings]} (first removed-elements)
                   key (element-key-fn fact bindings)
                   mp (match-pair fact id)]
               (into []
                     (keep (fn [token]
                             (when (= key (token-key-fn token))
                               (let [beta-bindings (if join-filter-fn
                                                     (join-node-matches node join-filter-fn token fact bindings {})
                                                     {})]
                                 (when (if join-filter-fn beta-bindings true)
                                   (->Token (conj (:matches token) mp)
                                            (conj (:bindings token) bindings beta-bindings)))))))
                     tokens))))
          ;; Original path
          (retract-tokens
           transport
           memory
           listener
           children
           (if (next removed-elements)
             (platform/eager-for [{:keys [fact bindings] :as _element} removed-elements
                                  :let [mp (match-pair fact id)]
                                  token tokens
                                  :let [beta-bindings (join-node-matches node join-filter-fn token fact bindings {})]
                                  :when beta-bindings]
                                 (->Token (conj (:matches token) mp)
                                          (conj (:bindings token) bindings beta-bindings)))
             (let [{:keys [fact bindings]} (first removed-elements)
                   mp (match-pair fact id)]
               (into []
                     (keep (fn [token]
                             (let [beta-bindings (join-node-matches node join-filter-fn token fact bindings {})]
                               (when beta-bindings
                                 (->Token (conj (:matches token) mp)
                                          (conj (:bindings token) bindings beta-bindings))))))
                     tokens))))))))

  IConditionNode
  (get-condition-description [_this]
    (let [{:keys [type constraints original-constraints]} condition
          full-constraints (if (seq original-constraints)
                             original-constraints
                             constraints)]
      (into [type] full-constraints))))

;; The NegationNode is a beta node in the Rete network that simply
;; negates the incoming tokens from its ancestors. It sends tokens
;; to its descendent only if the negated condition or join fails (is false).
(defrecord NegationNode [id condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (l/left-activate! listener node tokens)
    (mem/add-tokens! memory node join-bindings tokens)
    (when (empty? (mem/get-elements memory node join-bindings))
      (send-tokens transport memory listener children tokens)))

  (left-retract [node join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (mem/remove-tokens! memory node join-bindings tokens)
    (when (empty? (mem/get-elements memory node join-bindings))
      (retract-tokens transport memory listener children tokens)))

  (get-join-keys [_node] binding-keys)

  (description [_node] (str "NegationNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    ;; Immediately evaluate whether there are previous elements since mem/get-elements
    ;; returns a mutable list with a LocalMemory on the JVM currently.
    (let [previously-empty? (empty? (mem/get-elements memory node join-bindings))]
      (l/right-activate! listener node elements)
      (mem/add-elements! memory node join-bindings elements)
      ;; Retract tokens that matched the activation if no element matched the negation previously.
      ;; If an element matched the negation already then no elements were propagated and there is
      ;; nothing to retract.
      (when previously-empty?
        (retract-tokens transport memory listener children (mem/get-tokens memory node join-bindings)))))

  (right-retract [node join-bindings elements memory transport listener]
    (l/right-retract! listener node elements)
    (mem/remove-elements! memory node join-bindings elements)
    (when (empty? (mem/get-elements memory node join-bindings))
      (send-tokens transport memory listener children (mem/get-tokens memory node join-bindings))))

  IConditionNode
  (get-condition-description [_this]
    (let [{:keys [type constraints]} condition]
      [:not (into [type] constraints)])))

(defn- matches-some-facts?
  "Returns true if the given token matches one or more of the given elements."
  [node token elements join-filter-fn condition]
  (let [env (:env condition)]
    (some (fn [e]
            (join-node-matches node join-filter-fn token (:fact e) (:bindings e) env))
          elements)))

;; A specialization of the NegationNode that supports additional tests
;; that have to occur on the beta side of the network. The key difference between this and the simple
;; negation node is the join-filter-fn, which allows negation tests to
;; be applied with the parent token in context, rather than just a simple test of the non-existence
;; on the alpha side.
(defrecord NegationWithJoinFilterNode [id condition join-filter-fn children binding-keys element-key-fn token-key-fn]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (l/left-activate! listener node tokens)
    (mem/add-tokens! memory node join-bindings tokens)

    (send-tokens transport
                 memory
                 listener
                 children
                 (let [elements (mem/get-elements memory node join-bindings)]
                   (if (and element-key-fn (seq elements))
                     ;; Sub-indexed path: group elements by key, look up only candidates per token.
                     (let [element-index (group-by #(element-key-fn (:fact %) (:bindings %)) elements)
                           env (:env condition)]
                       (filterv (fn [token]
                                  (not (some #(join-node-matches node join-filter-fn token (:fact %) (:bindings %) env)
                                             (get element-index (token-key-fn token) []))))
                                tokens))
                     ;; Original path: linear scan.
                     (platform/eager-for [token tokens
                                          :when (not (matches-some-facts? node
                                                                          token
                                                                          elements
                                                                          join-filter-fn
                                                                          condition))]
                                         token)))))

  (left-retract [node join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (mem/remove-tokens! memory node join-bindings tokens)
    (retract-tokens transport
                    memory
                    listener
                    children

                    ;; Retract only if it previously had no matches in the negation node,
                    ;; and therefore had an activation.
                    (let [elements (mem/get-elements memory node join-bindings)]
                      (if (and element-key-fn (seq elements))
                        ;; Sub-indexed path.
                        (let [element-index (group-by #(element-key-fn (:fact %) (:bindings %)) elements)
                              env (:env condition)]
                          (filterv (fn [token]
                                     (not (some #(join-node-matches node join-filter-fn token (:fact %) (:bindings %) env)
                                                (get element-index (token-key-fn token) []))))
                                   tokens))
                        ;; Original path.
                        (platform/eager-for [token tokens
                                             :when (not (matches-some-facts? node
                                                                             token
                                                                             elements
                                                                             join-filter-fn
                                                                             condition))]
                                            token)))))

  (get-join-keys [_node] binding-keys)

  (description [_node] (str "NegationWithJoinFilterNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (l/right-activate! listener node elements)
    (let [previous-elements (mem/get-elements memory node join-bindings)]
      ;; Retract tokens that matched the activation, since they are no longer negated.
      (retract-tokens transport
                      memory
                      listener
                      children
                      (if token-key-fn
                        ;; Sub-indexed path: for each new element find candidate tokens by key,
                        ;; retract those that are newly blocked (matched now but not by any previous element).
                        (let [tokens (mem/get-tokens memory node join-bindings)
                              env (:env condition)]
                          (if (next elements)
                            ;; Batch: build token-index and prev-element-index once for all elements.
                            (let [token-index       (group-by token-key-fn tokens)
                                  prev-element-index (group-by #(element-key-fn (:fact %) (:bindings %)) previous-elements)]
                              (into []
                                    (comp
                                     (mapcat (fn [{:keys [fact bindings]}]
                                               (let [key (element-key-fn fact bindings)]
                                                 (keep (fn [token]
                                                         (when (join-node-matches node join-filter-fn token fact bindings env)
                                                           token))
                                                       (get token-index key [])))))
                                     (distinct)
                                     (remove (fn [token]
                                               (some #(join-node-matches node join-filter-fn token (:fact %) (:bindings %) env)
                                                     (get prev-element-index (token-key-fn token) [])))))
                                    elements))
                            ;; Single element: avoid group-by allocation — linear key-filter then join check.
                            (let [{:keys [fact bindings]} (first elements)
                                  key (element-key-fn fact bindings)
                                  prev-for-key (filterv #(= (element-key-fn (:fact %) (:bindings %)) key) previous-elements)]
                              (into []
                                    (keep (fn [token]
                                            (when (and (= (token-key-fn token) key)
                                                       (join-node-matches node join-filter-fn token fact bindings env)
                                                       (not (some #(join-node-matches node join-filter-fn token (:fact %) (:bindings %) env)
                                                                  prev-for-key)))
                                              token)))
                                    tokens))))
                        ;; Original path.
                        (platform/eager-for [token (mem/get-tokens memory node join-bindings)

                                             ;; Retract downstream if the token now has matching elements and didn't before.
                                             ;; We check the new elements first in the expectation that the new elements will be
                                             ;; smaller than the previous elements most of the time
                                             ;; and that the time to check the elements will be proportional
                                             ;; to the number of elements.
                                             :when (and (matches-some-facts? node
                                                                             token
                                                                             elements
                                                                             join-filter-fn
                                                                             condition)
                                                        (not (matches-some-facts? node
                                                                                  token
                                                                                  previous-elements
                                                                                  join-filter-fn
                                                                                  condition)))]
                                            token)))
      ;; Adding the elements will mutate the previous-elements since, on the JVM, the LocalMemory
      ;; currently returns a mutable List from get-elements after changes in issue 184.  We need to use the
      ;; new and old elements in the logic above as separate collections.  Therefore we need to delay updating the
      ;; memory with the new elements until after we are done with previous-elements.
      (mem/add-elements! memory node join-bindings elements)))

  (right-retract [node join-bindings elements memory transport listener]

    (l/right-retract! listener node elements)
    (mem/remove-elements! memory node join-bindings elements)

    (send-tokens transport
                 memory
                 listener
                 children
                 (let [remaining-elements (mem/get-elements memory node join-bindings)]
                   (if token-key-fn
                     ;; Sub-indexed path.
                     (let [tokens (mem/get-tokens memory node join-bindings)
                           env    (:env condition)]
                       (if (next elements)
                         ;; Batch: build indexes once for all retracted elements.
                         (let [token-index             (group-by token-key-fn tokens)
                               remaining-element-index (group-by #(element-key-fn (:fact %) (:bindings %)) remaining-elements)]
                           (into []
                                 (comp
                                  (mapcat (fn [{:keys [fact bindings]}]
                                            (let [key (element-key-fn fact bindings)]
                                              (keep (fn [token]
                                                      (when (join-node-matches node join-filter-fn token fact bindings env)
                                                        token))
                                                    (get token-index key [])))))
                                  (distinct)
                                  (remove (fn [token]
                                            (some #(join-node-matches node join-filter-fn token (:fact %) (:bindings %) env)
                                                  (get remaining-element-index (token-key-fn token) [])))))
                                 elements))
                         ;; Single element: avoid group-by allocation.
                         (let [{:keys [fact bindings]} (first elements)
                               key                     (element-key-fn fact bindings)
                               remaining-for-key       (filterv #(= (element-key-fn (:fact %) (:bindings %)) key) remaining-elements)]
                           (into []
                                 (keep (fn [token]
                                         (when (and (= (token-key-fn token) key)
                                                    (join-node-matches node join-filter-fn token fact bindings env)
                                                    (not (some #(join-node-matches node join-filter-fn token (:fact %) (:bindings %) env)
                                                               remaining-for-key)))
                                           token)))
                                 tokens))))
                     ;; Original path.
                     (platform/eager-for [token (mem/get-tokens memory node join-bindings)

                                          ;; Propagate tokens when some of the retracted facts joined
                                          ;; but none of the remaining facts do.
                                          :when (and (matches-some-facts? node
                                                                          token
                                                                          elements
                                                                          join-filter-fn
                                                                          condition)
                                                     (not (matches-some-facts? node
                                                                               token
                                                                               remaining-elements
                                                                               join-filter-fn
                                                                               condition)))]
                                         token)))))

  IConditionNode
  (get-condition-description [_this]
    (let [{:keys [type constraints original-constraints]} condition
          full-constraints (if (seq original-constraints)
                             original-constraints
                             constraints)]
      [:not (into [type] full-constraints)])))

(defn- test-node-matches
  [node test-handler env token]
  (let [test-result (try
                      (test-handler token env)
                      (catch #?(:clj Exception :cljs :default) e
                        (throw-condition-exception {:cause e
                                                    :node node
                                                    :env env
                                                    :bindings (:bindings token)})))]
    test-result))

;; The test node represents a Rete extension in which an arbitrary test condition is run
;; against bindings from ancestor nodes. Since this node
;; performs no joins it does not accept right activations or retractions.
(defrecord TestNode [id env constraints test children]
  ILeftActivate
  (left-activate [node _join-bindings tokens memory transport listener]
    (l/left-activate! listener node tokens)
    (send-tokens
     transport
     memory
     listener
     children
     (platform/eager-for
      [token tokens
       :when (test-node-matches node test env token)]
      token)))

  (left-retract [node _join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (retract-tokens transport memory listener children tokens))

  (get-join-keys [_node] [])

  (description [_node] (str "TestNode -- " (:text test)))

  IConditionNode
  (get-condition-description [_this]
    (into [:test] constraints)))

;; Constant fallback value for accum-reduced when no previous result exists.
;; Hoisted to avoid re-creating the vector + metadata on every accumulator activation.
(def ^:private no-accum-reduced-initial
  ^{::accum-node true} [::mem/no-accum-reduced ::not-reduced])

(defn- do-accumulate
  "Runs the actual accumulation.  Returns the accumulated value.
   Two-arity uses the accumulator's initial-value; three-arity uses the provided initial value
   to avoid allocating a new Accumulator record."
  ([accumulator facts]
   (r/reduce (:reduce-fn accumulator)
             (:initial-value accumulator)
             facts))
  ([accumulator initial-value facts]
   (r/reduce (:reduce-fn accumulator)
             initial-value
             facts)))

(defn- retract-accumulated
  "Helper function to retract an accumulated value."
  [node _accum-condition _accumulator result-binding token converted-result fact-bindings transport memory listener]
  (let [new-facts (conj (:matches token) (match-pair converted-result (:id node)))
        new-bindings (cond-> (conj (:bindings token) fact-bindings)
                       result-binding (assoc result-binding converted-result))]

    (retract-tokens transport memory listener (:children node)
                    [(->Token new-facts new-bindings)])))

(defn- send-accumulated
  "Helper function to send the result of an accumulated value to the node's children."
  [node _accum-condition _accumulator result-binding token converted-result fact-bindings transport memory listener]
  (let [new-bindings (cond-> (conj (:bindings token) fact-bindings)
                       result-binding (assoc result-binding converted-result))

        ;; This is to check that the produced accumulator result is
        ;; consistent with any variable from another rule condition
        ;; that has the same binding. If another condition binds something
        ;; to ?x, only the accumulator results that match that would propagate.
        ;; We can do this safely because previous states get retracted.
        previous-result (get (:bindings token) result-binding ::no-previous-result)]

    (when (or (= previous-result ::no-previous-result)
              (= previous-result converted-result))

      (send-tokens transport memory listener (:children node)
                   [(->Token (conj (:matches token) (match-pair converted-result (:id node))) new-bindings)]))))

;; The AccumulateNode hosts Accumulators, a Rete extension described above, in the Rete network.
;; It behaves similarly to a JoinNode, but performs an accumulation function on the incoming
;; working-memory elements before sending a new token to its descendents.
(defrecord AccumulateNode [id accum-condition accumulator result-binding children binding-keys new-bindings]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    (l/left-activate! listener node tokens)
    (let [previous-results (mem/get-accum-reduced-all memory node join-bindings)
          convert-return-fn (:convert-return-fn accumulator)
          has-matches? (seq previous-results)
          initial-value (when-not has-matches?
                          (:initial-value accumulator))
          initial-converted (when (some? initial-value)
                              (convert-return-fn initial-value))]

      (mem/add-tokens! memory node join-bindings tokens)

      (cond
        ;; If there are previously accumulated results to propagate, use them.  If this is the
        ;; first time there are matching tokens, then the reduce will have to happen for the
        ;; first time.  However, this reduce operation is independent of the specific tokens
        ;; since the elements join to the tokens via pre-computed hash join bindings for this
        ;; node.  So only reduce once per binding grouped facts, for all tokens. This includes
        ;; all bindings, not just the join bindings.
        has-matches?
        (doseq [[fact-bindings [previous previous-reduced]] previous-results
                :let [first-reduce? (= ::not-reduced previous-reduced)
                      previous-reduced (if first-reduce?
                                         ;; Need to accumulate since this is the first time we have
                                         ;; tokens matching so we have not accumulated before.
                                         (do-accumulate accumulator previous)
                                         previous-reduced)
                      accum-reduced (when first-reduce?
                                      ^::accum-node [previous previous-reduced])
                      converted (when (some? previous-reduced)
                                  (convert-return-fn previous-reduced))]]

          ;; Newly accumulated results need to be added to memory.
          (when first-reduce?
            (l/add-accum-reduced! listener node join-bindings accum-reduced fact-bindings)
            (mem/add-accum-reduced! memory node join-bindings accum-reduced fact-bindings))

          (when (some? converted)
            (doseq [token tokens]
              (send-accumulated node accum-condition accumulator result-binding token converted fact-bindings
                                transport memory listener))))

        ;; There are no previously accumulated results, but we still may need to propagate things
        ;; such as a sum of zero items.
        ;; If an initial value is provided and the converted value is non-nil, we can propagate
        ;; the converted value as the accumulated item.
        (and (some? initial-converted)
             (empty? new-bindings))

        ;; Note that this is added to memory a single time for all matching tokens because the memory
        ;; location doesn't depend on bindings from individual tokens.

        (let [accum-reduced ^::accum-node [[] initial-value]]
          ;; The fact-bindings are normally a superset of the join-bindings.  We have no fact-bindings
          ;; that are not join-bindings in this case since we have verified that new-bindings is empty.
          ;; Therefore the join-bindings and fact-bindings are exactly equal.
          (l/add-accum-reduced! listener node join-bindings accum-reduced join-bindings)
          (mem/add-accum-reduced! memory node join-bindings accum-reduced join-bindings)

          ;; Send the created accumulated item to the children for each token.
          (doseq [token tokens]
            (send-accumulated node accum-condition accumulator result-binding token initial-converted {}
                              transport memory listener)))

        ;; Propagate nothing if the above conditions don't apply.
        :else
        nil)))

  (left-retract [node join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (doseq [:let [removed-tokens (mem/remove-tokens! memory node join-bindings tokens)
                  remaining-tokens (mem/get-tokens memory node join-bindings)

                  ;; Note:  Memory *must* be read here before the memory is potentially cleared in the
                  ;; following lines.
                  previous-results (mem/get-accum-reduced-all memory node join-bindings)

                  ;; If there are no new bindings created by the accumulator condition then
                  ;; a left-activation can create a new binding group in the accumulator memory.
                  ;; If this token is later removed without corresponding elements having been added,
                  ;; we remove the binding group from the accum memory.  Otherwise adding and then retracting
                  ;; tokens could force bindings to retained for the duration of the JVM, regardless of whether
                  ;; the backing facts were garbage collectable.  This would be a memory leak.
                  _ (when (and (empty? remaining-tokens)
                               (empty? new-bindings)
                               (let [current (mem/get-accum-reduced memory node join-bindings join-bindings)]
                                 (and
                                  ;; If there is nothing under these bindings already in the memory then there is no
                                  ;; need to take further action.
                                  (not= current ::mem/no-accum-reduced)
                                  ;; Check to see if there are elements under this binding group.
                                  ;; If elements are present we must keep the binding group regardless of the
                                  ;; presence or absence of tokens.
                                  (-> current first empty?))))
                      (mem/remove-accum-reduced! memory node join-bindings join-bindings))]
            ;; There is nothing to do if no tokens were removed.
            :when (seq removed-tokens)
            ;; Note that this will cause a Cartesian join between tokens and elements groups where the token
            ;; and element group share the same join bindings, but the element groups may have additional bindings
            ;; that come from their alpha nodes. Keep in mind that these element groups need elements to be created
            ;; and cannot come from initial values if they have bindings that are not shared with tokens.
            [fact-bindings [_previous previous-reduced]] previous-results
            :let [;; If there were tokens before that are now removed, the value would have been accumulated already.
                  ;; This means there is no need to check for ::not-reduced here.
                  previous-converted (when (some? previous-reduced)
                                       ((:convert-return-fn accumulator) previous-reduced))]
            ;; A nil previous result should not have been propagated before.
            :when (some? previous-converted)
            token removed-tokens]
      (retract-accumulated node accum-condition accumulator result-binding token previous-converted fact-bindings
                           transport memory listener)))

  (get-join-keys [_node] binding-keys)

  (description [_node] (str "AccumulateNode -- " accumulator))

  IAccumRightActivate
  (pre-reduce [_node elements]
    ;; Return a seq tuples with the form [binding-group facts-from-group-elements].
    (platform/eager-for [[bindings element-group] (platform/group-by-seq :bindings elements)]
                        [bindings (mapv :fact element-group)]))

  (right-activate-reduced [node join-bindings fact-seq memory transport listener]

    ;; Combine previously reduced items together, join to matching tokens, and emit child tokens.
    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  ;; Note that we want to iterate over all tokens with the desired join bindings later
                  ;; independently of the fact binding groups created by elements; that is, a token
                  ;; can join with multiple groups of fact bindings when the accumulator condition
                  ;; creates new bindings.
                  matched-tokens (mem/get-tokens memory node join-bindings)
                  has-matches? (seq matched-tokens)]
            [bindings facts] fact-seq
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)
                  has-previous? (not= ::mem/no-accum-reduced previous)
                  [previous previous-reduced] (if has-previous?
                                                previous
                                                no-accum-reduced-initial)
                  combined (if has-previous?
                             (into previous facts)
                             facts)
                  combined-reduced
                  (cond
                    ;; Reduce all of the combined items for the first time if there are
                    ;; now matches, and nothing was reduced before.
                    (and has-matches?
                         (= ::not-reduced previous-reduced))
                    (do-accumulate accumulator combined)

                    ;; There are matches, a previous reduced value for the previous items and a
                    ;; :combine-fn is given.  Use the :combine-fn on both the previously reduced
                    ;; and the newly reduced results.
                    (and has-matches?
                         (:combine-fn accumulator))
                    ((:combine-fn accumulator) previous-reduced (do-accumulate accumulator facts))

                    ;; There are matches and there is a previous reduced value for the previous
                    ;; items.  So just add the new items to the accumulated value.
                    has-matches?
                    (do-accumulate accumulator previous-reduced facts)

                    ;; There are no matches right now.  So do not perform any accumulations.
                    ;; If there are never matches, time will be saved by never reducing.
                    :else
                    ::not-reduced)

                  converted (when (and (some? combined-reduced)
                                       (not= ::not-reduced combined-reduced))
                              (convert-return-fn combined-reduced))

                  previous-converted (when (and has-previous?
                                                (some? previous-reduced)
                                                (not= ::not-reduced previous-reduced))
                                       (convert-return-fn previous-reduced))

                  accum-reduced ^::accum-node [combined combined-reduced]]]

      ;; Add the combined results to memory.
      (l/add-accum-reduced! listener node join-bindings accum-reduced bindings)
      (mem/add-accum-reduced! memory node join-bindings accum-reduced bindings)

      (cond

        ;; Do nothing when the result was nil before and after.
        (and (nil? previous-converted)
             (nil? converted))
        nil

        (nil? converted)
        (doseq [token matched-tokens]
          (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                               transport memory listener))

        (nil? previous-converted)
        (doseq [token matched-tokens]
          (send-accumulated node accum-condition accumulator result-binding token converted bindings
                            transport memory listener))


        ;; If there are previous results, then propagate downstream if the new result differs from
        ;; the previous result.  If the new result is equal to the previous result don't do
        ;; anything.  Note that the memory has already been updated with the new combined value,
        ;; which may be needed if elements in memory changes later.
        (not= converted previous-converted)
        ;; There is no requirement that we doseq over all retractions then doseq over propagations; we could
        ;; just as easily doseq over tokens at the top level and retract and propagate for each token in turn.
        ;; In the absence of hard evidence either way, doing it this way is just an educated guess as to
        ;; which is likely to be more performant.
        (do
          (doseq [token matched-tokens]
            (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                 transport memory listener))
          (doseq [token matched-tokens]
            (send-accumulated node accum-condition accumulator result-binding token converted bindings
                              transport memory listener))))))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    (l/right-activate! listener node elements)
    ;; Simple right-activate implementation simple defers to
    ;; accumulator-specific logic.
    (right-activate-reduced
     node
     join-bindings
     (pre-reduce node elements)
     memory
     transport
     listener))

  (right-retract [node join-bindings elements memory transport listener]

    (l/right-retract! listener node elements)

    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  ;; As in right-activate-reduced, a token can match with multiple groupings of elements
                  ;; by their bindings.
                  matched-tokens (mem/get-tokens memory node join-bindings)
                  has-matches? (seq matched-tokens)]
            [bindings elements] (platform/group-by-seq :bindings elements)

            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)
                  has-previous? (not= ::mem/no-accum-reduced previous)
                  [previous previous-reduced] (if has-previous?
                                                previous
                                                no-accum-reduced-initial)]

            ;; No need to retract anything if there were no previous items.
            :when has-previous?

            ;; Compute the new version with the retracted information.
            :let [facts (mapv :fact elements)
                  [removed retracted] (mem/remove-first-of-each facts previous)
                  all-retracted? (empty? retracted)
                  ;; If there is a previous and matches, there would have been a
                  ;; propagated and accumulated value.  So there is something to
                  ;; retract and re-accumulated in place of.
                  ;; Otherwise, no reduce is needed right now.
                  retracted-reduced (if (and has-matches?
                                             (not all-retracted?))
                                      ;; Use the provided :retract-fn if one is provided.
                                      ;; Otherwise, just re-accumulate based on the
                                      ;; remaining items after retraction.
                                      (if-let [retract-fn (:retract-fn accumulator)]
                                        (r/reduce retract-fn previous-reduced removed)
                                        (do-accumulate accumulator retracted))
                                      ::not-reduced)

                  ;; It is possible that either the retracted or previous reduced are ::not-reduced
                  ;; at this point if there are no matching tokens.  has-matches? indicates this.  If
                  ;; this is the case, there are no converted values to calculate.  However, memory still
                  ;; will be updated since the facts left after this retraction still need to be stored
                  ;; for later possible activations.
                  retracted-converted (when (and (some? retracted-reduced)
                                                 (not= ::not-reduced retracted-reduced))
                                        (convert-return-fn retracted-reduced))
                  previous-converted (when (and (some? previous-reduced)
                                                (not= ::not-reduced previous-reduced))
                                       (convert-return-fn previous-reduced))]]

      (if all-retracted?
        (do
          ;; When everything has been retracted we need to remove the accumulated results from memory.
          (l/remove-accum-reduced! listener node join-bindings bindings)
          (mem/remove-accum-reduced! memory node join-bindings bindings)

          (doseq [:when (some? previous-converted)
                  token matched-tokens]
            ;; Retract the previous token.
            (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                 transport memory listener))

          (let [initial-value (:initial-value accumulator)

                initial-converted (when initial-value
                                    (convert-return-fn initial-value))]

            (when (and (some? initial-converted)
                       (empty? new-bindings))

              (doseq [token matched-tokens]
                (l/add-accum-reduced! listener node join-bindings ^::accum-node [[] initial-value] join-bindings)
                (mem/add-accum-reduced! memory node join-bindings ^::accum-node [[] initial-value] join-bindings)
                (send-accumulated node accum-condition accumulator result-binding token initial-converted {}
                                  transport memory listener)))))
        (do
          ;; Add our newly retracted information to our node.
          (l/add-accum-reduced! listener node join-bindings ^::accum-node [retracted retracted-reduced] bindings)
          (mem/add-accum-reduced! memory node join-bindings ^::accum-node  [retracted retracted-reduced] bindings)

          (cond
            (and (nil? previous-converted)
                 (nil? retracted-converted))
            nil

            (nil? previous-converted)
            (doseq [token matched-tokens]
              (send-accumulated node accum-condition accumulator result-binding token retracted-converted bindings
                                transport memory listener))

            (nil? retracted-converted)
            (doseq [token matched-tokens]
              (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                   transport memory listener))

            (not= retracted-converted previous-converted)
            ;; There is no requirement that we doseq over all retractions then doseq over propagations; we could
            ;; just as easily doseq over tokens at the top level and retract and propagate for each token in turn.
            ;; In the absence of hard evidence either way, doing it this way is just an educated guess as to
            ;; which is likely to be more performant.
            (do
              (doseq [token matched-tokens]
                (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                     transport memory listener))
              (doseq [token matched-tokens]
                (send-accumulated node accum-condition accumulator result-binding token retracted-converted bindings
                                  transport memory listener))))))))

  IConditionNode
  (get-condition-description [_this]
    (let [{:keys [accumulator from]} accum-condition
          {:keys [type constraints]} from
          condition (into [type] constraints)
          result-symbol (symbol (name result-binding))]
      [result-symbol '<- accumulator :from condition]))

  IAccumInspect
  (token->matching-elements [this memory token]
    ;; Tokens are stored in the memory keyed on join bindings with previous nodes and new bindings
    ;; introduced in this node.  Each of these sets of bindings is known at the time of rule network
    ;; compilation.  It is expected that this function will receive tokens that were propagated from this
    ;; node to its children and may have had other bindings added in the process.  The bindings map entries
    ;; in the tokens created by descendants based on tokens propagated from ancestors are subsets of the bindings
    ;; in each ancestor.  Put differently, if token T1 is passed to a child that create a token T2 based on it
    ;; and passes it to its children, the following statement is true:
    ;; (= (select-keys (-> t1 :bindings keys) t2)
    ;;    (:bindings t1))
    ;; This being the case, we can use the downstream token to find out what binding key-value pairs were used
    ;; to create the token "stream" of which it is part.
    (let [join-bindings (-> token :bindings (select-keys (get-join-keys this)))
          fact-bindings (-> token :bindings (select-keys new-bindings))]
      (first (mem/get-accum-reduced memory this join-bindings (conj join-bindings fact-bindings))))))

(defn- filter-accum-facts
  "Run a filter on elements against a given token for constraints that are not simple hash joins."
  [node join-filter-fn token candidate-facts bindings]
  (filterv #(join-node-matches node join-filter-fn token % bindings {}) candidate-facts))

;; A specialization of the AccumulateNode that supports additional tests
;; that have to occur on the beta side of the network. The key difference between this and the simple
;; accumulate node is the join-filter-fn, which accepts a token and a fact and filters out facts that
;; are not consistent with the given token.
(defrecord AccumulateWithJoinFilterNode [id accum-condition accumulator join-filter-fn
                                         result-binding children binding-keys new-bindings]

  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]

    (l/left-activate! listener node tokens)

    ;; Facts that are candidates for matching the token are used in this accumulator node,
    ;; which must be filtered before running the accumulation.
    (let [convert-return-fn (:convert-return-fn accumulator)
          grouped-candidate-facts (mem/get-accum-reduced-all memory node join-bindings)]
      (mem/add-tokens! memory node join-bindings tokens)

      (cond

        (seq grouped-candidate-facts)
        (doseq [token tokens
                [fact-bindings candidate-facts] grouped-candidate-facts

                ;; Filter to items that match the incoming token, then apply the accumulator.
                :let [filtered-facts (filter-accum-facts node join-filter-fn token candidate-facts fact-bindings)]

                :when (or (seq filtered-facts)
                          ;; Even if there no filtered facts, if there are no new bindings we may
                          ;; have an initial value to propagate.
                          (and (some? (:initial-value accumulator))
                               (empty? new-bindings)))

                :let [accum-result (do-accumulate accumulator filtered-facts)
                      converted-result (when (some? accum-result)
                                         (convert-return-fn accum-result))]

                :when (some? converted-result)]

          (send-accumulated node accum-condition accumulator result-binding token
                            converted-result fact-bindings transport memory listener))

        ;; There are no previously accumulated results, but we still may need to propagate things
        ;; such as a sum of zero items.
        ;; If all variables in the accumulated item are bound and an initial
        ;; value is provided, we can propagate the initial value as the accumulated item.

        ;; We need to not propagate nil initial values, regardless of whether the convert-return-fn
        ;; makes them non-nil, in order to not break existing code; this is discussed more in the
        ;; right-activate-reduced implementation.
        (and (some? (:initial-value accumulator))
             (empty? new-bindings)) ; An initial value exists that we can propagate.
        (let [initial-value (:initial-value accumulator)
              ;; Note that we check the the :initial-value is non-nil above, which is why we
              ;; don't need (when initial-value (convert-return-fn initial-value)) here.
              converted-result (convert-return-fn initial-value)]

          (when (some? converted-result)
            ;; Send the created accumulated item to the children.
            (doseq [token tokens]
              (send-accumulated node accum-condition accumulator result-binding token
                                converted-result join-bindings transport memory listener))))

        ;; Propagate nothing if the above conditions don't apply.
        :else nil)))

  (left-retract [node join-bindings tokens memory transport listener]

    (l/left-retract! listener node tokens)

    (let [;; Even if the accumulator didn't propagate anything before we still need to remove the tokens
          ;; in case they would have otherwise been used in the future.
          tokens (mem/remove-tokens! memory node join-bindings tokens)
          convert-return-fn (:convert-return-fn accumulator)
          grouped-candidate-facts (mem/get-accum-reduced-all memory node join-bindings)]

      (cond

        (seq grouped-candidate-facts)
        (doseq [token tokens
                [fact-bindings candidate-facts] grouped-candidate-facts

                :let [filtered-facts (filter-accum-facts node join-filter-fn token candidate-facts fact-bindings)]

                :when (or (seq filtered-facts)
                          ;; Even if there no filtered facts, if there are no new bindings an initial value
                          ;; maybe have propagated, and if so we need to retract it.
                          (and (some? (:initial-value accumulator))
                               (empty? new-bindings)))

                :let [accum-result (do-accumulate accumulator filtered-facts)
                      retracted-converted (when (some? accum-result)
                                            (convert-return-fn accum-result))]

                ;; A nil retracted previous result should not have been propagated before.
                :when (some? retracted-converted)]

          (retract-accumulated node accum-condition accumulator result-binding token
                               retracted-converted fact-bindings transport memory listener))

        (and (some? (:initial-value accumulator))
             (empty? new-bindings))
        (let [initial-value (:initial-value accumulator)
              ;; Note that we check the the :initial-value is non-nil above, which is why we
              ;; don't need (when initial-value (convert-return-fn initial-value)) here.
              converted-result (convert-return-fn initial-value)]

          (when (some? converted-result)
            (doseq [token tokens]
              (retract-accumulated node accum-condition accumulator result-binding token
                                   converted-result join-bindings transport memory listener))))

        :else nil)))

  (get-join-keys [_node] binding-keys)

  (description [_node] (str "AccumulateWithBetaPredicateNode -- " accumulator))

  IAccumRightActivate
  (pre-reduce [_node elements]
    ;; Return a map of bindings to the candidate facts that match them. This accumulator
    ;; depends on the values from parent facts, so we defer actually running the accumulator
    ;; until we have a token.
    (platform/eager-for [[bindings element-group] (platform/group-by-seq :bindings elements)]
                        [bindings (mapv :fact element-group)]))

  (right-activate-reduced [node join-bindings binding-candidates-seq memory transport listener]

    ;; Combine previously reduced items together, join to matching tokens,
    ;; and emit child tokens.
    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings candidates] binding-candidates-seq
            :let [previous-candidates (mem/get-accum-reduced memory node join-bindings bindings)
                  previously-reduced? (not= ::mem/no-accum-reduced previous-candidates)
                  previous-candidates (when previously-reduced? previous-candidates)]]

      ;; Combine the newly reduced values with any previous items.  Ensure that new items are always added to the end so that
      ;; we have a consistent order for retracting results from accumulators such as acc/all whose results can be in any order.  Making this
      ;; ordering consistent allows us to skip the filter step on previous elements on right-activations.
      (let [combined-candidates (if previous-candidates
                                  (into previous-candidates candidates)
                                  candidates)]

        (l/add-accum-reduced! listener node join-bindings combined-candidates bindings)

        (mem/add-accum-reduced! memory node join-bindings combined-candidates bindings))

      (doseq [token matched-tokens

              :let [new-filtered-facts (filter-accum-facts node join-filter-fn token candidates bindings)]

              ;; If no new elements matched the token, we don't need to do anything for this token
              ;; since the final result is guaranteed to be the same.
              :when (seq new-filtered-facts)

              :let [previous-filtered-facts (filter-accum-facts node join-filter-fn token previous-candidates bindings)

                    previous-accum-result-init (cond
                                                 (seq previous-filtered-facts)
                                                 (do-accumulate accumulator previous-filtered-facts)

                                                 (and (-> accumulator :initial-value some?)
                                                      (empty? new-bindings))
                                                 (:initial-value accumulator)

                                                 ;; Allow direct determination later of whether there was a previous value
                                                 ;; as determined by the preceding cond conditions.
                                                 :else ::no-previous-value)

                    previous-accum-result (when (not= previous-accum-result-init ::no-previous-value)
                                            previous-accum-result-init)

                    ;; Since the new elements are added onto the end of the previous elements in the accum-memory
                    ;; accumulating using the new elements on top of the previous result is an accumulation in the same
                    ;; order as the elements are present in memory.  As a result, future accumulations on the contents of the accum memory
                    ;; prior to further modification of that memory will return the same result as here.  This is important since if we use
                    ;; something like acc/all to accumulate to and propagate [A B] if B is retracted we need to retract [A B] not [B A]; the latter won't
                    ;; actually retract anything, which would be invalid.
                    accum-result (if (not= previous-accum-result-init ::no-previous-value)
                                   ;; If there was a previous result, use it as the initial value
                                   ;; without allocating a new Accumulator record.
                                   (do-accumulate accumulator previous-accum-result new-filtered-facts)
                                   ;; If there was no previous result, use the default initial value.
                                   ;; Note that if there is a non-nil initial value but there are new binding
                                   ;; groups we consider there to have been no previous value, but we still want
                                   ;; to use the actual initial value, not nil.
                                   (do-accumulate accumulator new-filtered-facts))

                    previous-converted (when (some? previous-accum-result)
                                         (convert-return-fn previous-accum-result))

                    new-converted (when (some? accum-result)
                                    (convert-return-fn accum-result))]]

        (cond

          ;; When both the new and previous result were nil do nothing.
          (and (nil? previous-converted)
               (nil? new-converted))
          nil

          (nil? new-converted)
          (retract-accumulated node accum-condition accumulator result-binding token
                               previous-converted bindings transport memory listener)

          (nil? previous-converted)
          (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener)

          (not= new-converted previous-converted)
          (do
            (retract-accumulated node accum-condition accumulator result-binding token
                                 previous-converted bindings transport memory listener)
            (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener))))))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    (l/right-activate! listener node elements)
    ;; Simple right-activate implementation simple defers to
    ;; accumulator-specific logic.
    (right-activate-reduced
     node
     join-bindings
     (pre-reduce node elements)
     memory
     transport
     listener))

  (right-retract [node join-bindings elements memory transport listener]

    (l/right-retract! listener node elements)

    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings elements] (platform/group-by-seq :bindings elements)
            :let [previous-candidates (mem/get-accum-reduced memory node join-bindings bindings)]

            ;; No need to retract anything if there was no previous item.
            :when (not= ::mem/no-accum-reduced previous-candidates)

            :let [facts (mapv :fact elements)
                  [removed-candidates new-candidates] (mem/remove-first-of-each facts previous-candidates)]]

      ;; Add the new candidates to our node.
      (l/add-accum-reduced! listener node join-bindings new-candidates bindings)
      (mem/add-accum-reduced! memory node join-bindings new-candidates bindings)

      (doseq [;; Get all of the previously matched tokens so we can retract and re-send them.
              token matched-tokens

              :let [previous-facts   (filter-accum-facts node join-filter-fn token previous-candidates bindings)
                    retract-fn       (:retract-fn accumulator)
                    ;; When retract-fn is present and some candidates remain, compute which of the
                    ;; removed candidates pass the join filter.  This lets us apply retract-fn
                    ;; incrementally instead of re-accumulating from scratch.
                    removed-matching (when (and retract-fn (seq new-candidates))
                                       (filter-accum-facts node join-filter-fn token removed-candidates bindings))
                    ;; Only compute new-facts on the original path (no retract-fn or all candidates gone).
                    new-facts        (when-not removed-matching
                                       (filter-accum-facts node join-filter-fn token new-candidates bindings))]

              ;; Guard: something changed that passed the join filter.
              ;; Incremental path: at least one removed candidate matched the filter.
              ;; Original path: count changed (previous is a superset of new after retraction).
              :when (if removed-matching
                      (seq removed-matching)
                      (not= (count previous-facts) (count new-facts)))

              :let [;; We know from the check above that matching elements existed previously,
                    ;; since if there were no previous matching elements the count of matching
                    ;; elements before and after a right-retraction cannot be different.
                    previous-result (do-accumulate accumulator previous-facts)

                    new-result (if removed-matching
                                 ;; Incremental path: apply retract-fn to each removed matching fact.
                                 ;; Avoids a full re-accumulate when only a few facts were retracted.
                                 (r/reduce retract-fn previous-result removed-matching)
                                 ;; Original path: re-accumulate from remaining facts, or use
                                 ;; initial-value/nil when all candidates are gone.
                                 (cond
                                   (seq new-facts)
                                   (do-accumulate accumulator new-facts)

                                   (and (-> accumulator :initial-value some?)
                                        (empty? new-bindings))
                                   (:initial-value accumulator)

                                   :else nil))

                    previous-converted (when (some? previous-result)
                                         (convert-return-fn previous-result))

                    new-converted (when (some? new-result)
                                    (convert-return-fn new-result))]]

        (cond

          ;; When both the previous and new results are nil do nothing.
          (and (nil? previous-converted)
               (nil? new-converted))
          nil

          (nil? new-converted)
          (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings transport memory listener)

          (nil? previous-converted)
          (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener)

          (not= previous-converted new-converted)
          (do
            (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings transport memory listener)
            (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener))))))

  IConditionNode
  (get-condition-description [_this]
    (let [{:keys [accumulator from]} accum-condition
          {:keys [type constraints original-constraints]} from
          result-symbol (symbol (name result-binding))
          full-constraints (if (seq original-constraints)
                             original-constraints
                             constraints)
          condition (into [type] full-constraints)]
      [result-symbol '<- accumulator :from condition]))

  ;; The explanation of the implementation of token->matching-elements on AccumulateNode applies here as well.
  ;; Note that since we store all facts propagated from the alpha network to this condition in the accum memory,
  ;; regardless of whether they meet the join condition with upstream facts from the beta network, we rerun the
  ;; the join filter function.  Since the :matches are not used in the join filter function and the bindings in the
  ;; token will contain all bindings used in the "ancestor token" to join with these same facts, we can just pass the token
  ;; as-is to the join filter.
  IAccumInspect
  (token->matching-elements [this memory token]
    (let [join-bindings (-> token :bindings (select-keys (get-join-keys this)))
          fact-bindings (-> token :bindings (select-keys new-bindings))
          unfiltered-facts (mem/get-accum-reduced memory this join-bindings (conj join-bindings fact-bindings))]
      ;; The functionality to throw conditions with meaningful information assumes that all bindings in the token
      ;; are meaningful to the join, which is not the case here since the token passed is from a descendant of this node, not
      ;; this node.  The generated error message also wouldn't make much sense in the context of session inspection.
      ;; We could create specialized error handling here, but in reality most cases that cause errors here would also cause
      ;; errors at rule firing time so the benefit would be limited.  Nevertheless there would be some benefit and it is
      ;; possible that we will do it in the future..
      (filter (fn [fact] (join-filter-fn token fact fact-bindings {}))
              unfiltered-facts))))

;; This lives here as it is both close to the node that it represents, and is accessible to both clj and cljs
(def node-type->abbreviated-type
  "To minimize function name length and attempt to prevent issues with filename length we can use these abbreviations to
  shorten the node types. Used during compilation of the rules network."
  {"AlphaNode" "AN"
   "TestNode" "TN"
   "AccumulateNode" "AccN"
   "AccumulateWithJoinFilterNode" "AJFN"
   "ProductionNode" "PN"
   "NegationWithJoinFilterNode" "NJFN"
   "ExpressionJoinNode" "EJN"})

(defn variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (platform/eager-for [item (tree-seq coll? seq expression)
                                 :when (and (symbol? item)
                                            (= \? (first (name item))))]
                                (keyword item))))

(defn conj-rulebases
  "DEPRECATED. Simply concat sequences of rules and queries.

   Conjoin two rulebases, returning a new one with the same rules."
  [base1 base2]
  (concat base1 base2))

;; Record for the per-activation rule context, replacing a per-activation map allocation.
;; Supports keyword lookup, destructuring, and get-in — fully compatible with all access patterns.
;; Defined here (before the PHREAK section) so fire-rules-phreak* can use ->RuleContext.
(defrecord RuleContext [token node batched-logical-insertions batched-unconditional-insertions batched-rhs-retractions])

;;; ============================================================
;;; PHREAK Phase 3.2 — Delta evaluation (pull phase)
;;;
;;; Each IDeltaEvaluate implementation:
;;;   1. Reads its element/token deltas from delta-memory.
;;;   2. Applies them to working memory.
;;;   3. Computes downstream token inserts/retracts.
;;;   4. Sends them via DeltaTransport (which queues rather
;;;      than cascades) so children are marked dirty and
;;;      evaluated later in the same pull-phase pass.
;;;
;;; Ordering contract: element deltas are processed before token
;;; deltas.  This means that when a token delta is processed the
;;; element memory is already up to date, so the join against
;;; existing elements produces the correct result without
;;; double-counting new-element × new-token pairs.
;;; ============================================================

;; ---------------------------------------------------------------------------
;; RootJoinNode — converts element deltas into token deltas for children.
;; No left/token side; the empty token is implicit.

(extend-type RootJoinNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (let [node-id  (:id node)
          children (:children node)]
      ;; Process element ops in ARRIVAL ORDER to preserve insert/retract semantics.
      (doseq [[insert? join-bindings elems] (dm/get-element-ops delta-memory node-id)]
        (if insert?
          ;; element inserts → add to memory, generate tokens, push downstream
          (do
            (mem/add-elements! memory node join-bindings elems)
            (send-tokens transport memory listener children
                         (into [] (map (fn [{:keys [fact bindings]}]
                                         (->Token [(match-pair fact node-id)] bindings)))
                               elems)))
          ;; element retracts → remove from memory, retract corresponding tokens
          (let [removed (mem/remove-elements! memory node join-bindings elems)]
            (when (seq removed)
              (retract-tokens transport memory listener children
                              (into [] (map (fn [{:keys [fact bindings]}]
                                              (->Token [(match-pair fact node-id)] bindings)))
                                    removed)))))))))

;; ---------------------------------------------------------------------------
;; HashJoinNode — custom evaluate-delta that avoids the demand-pull logic
;; present in right-activate (which would add tokens to regular memory before
;; the token-delta pass, causing duplicates when left-activate is later called).

(extend-type HashJoinNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (let [node-id  (:id node)
          children (:children node)]

      ;; 1. Element ops in ARRIVAL ORDER: join with existing tokens.
      ;;    Token ops not yet in regular memory → no overlap.
      (doseq [[insert? join-bindings elems] (dm/get-element-ops delta-memory node-id)]
        (if insert?
          (let [existing-tokens (mem/get-tokens memory node join-bindings)]
            (mem/add-elements! memory node join-bindings elems)
            (when (seq existing-tokens)
              (send-tokens
               transport memory listener children
               (if (next elems)
                 (platform/eager-for [{:keys [fact bindings]} elems
                                      :let [mp (match-pair fact node-id)]
                                      token existing-tokens]
                                     (->Token (conj (:matches token) mp)
                                              (conj (:bindings token) bindings)))
                 (let [{:keys [fact bindings]} (first elems)
                       mp (match-pair fact node-id)]
                   (into [] (map (fn [token]
                                   (->Token (conj (:matches token) mp)
                                            (conj (:bindings token) bindings))))
                         existing-tokens))))))
          (let [existing-tokens (mem/get-tokens memory node join-bindings)
                removed (mem/remove-elements! memory node join-bindings elems)]
            (when (and (seq removed) (seq existing-tokens))
              (retract-tokens
               transport memory listener children
               (if (next removed)
                 (platform/eager-for [{:keys [fact bindings]} removed
                                      :let [mp (match-pair fact node-id)]
                                      token existing-tokens]
                                     (->Token (conj (:matches token) mp)
                                              (conj (:bindings token) bindings)))
                 (let [{:keys [fact bindings]} (first removed)
                       mp (match-pair fact node-id)]
                   (into [] (map (fn [token]
                                   (->Token (conj (:matches token) mp)
                                            (conj (:bindings token) bindings))))
                         existing-tokens))))))))

      ;; 2. Token ops in ARRIVAL ORDER: join with CURRENT elements
      ;;    (element memory already updated in step 1).
      (doseq [[insert? join-bindings toks] (dm/get-token-ops delta-memory node-id)]
        (if insert?
          (do
            (mem/add-tokens! memory node join-bindings toks)
            (let [elements (mem/get-elements memory node join-bindings)]
              (when (seq elements)
                (send-tokens
                 transport memory listener children
                 (if (next toks)
                   (platform/eager-for [elem elements
                                        :let [fact (:fact elem)
                                              fb   (:bindings elem)
                                              mp   (match-pair fact node-id)]
                                        token toks]
                                       (->Token (conj (:matches token) mp)
                                                (conj (:bindings token) fb)))
                   (let [token (first toks)]
                     (into [] (map (fn [elem]
                                     (->Token (conj (:matches token)
                                                    (match-pair (:fact elem) node-id))
                                              (conj (:bindings token) (:bindings elem)))))
                           elements)))))))
          (let [removed  (mem/remove-tokens! memory node join-bindings toks)
                elements (mem/get-elements memory node join-bindings)]
            (when (and (seq removed) (seq elements))
              (retract-tokens
               transport memory listener children
               (if (next removed)
                 (platform/eager-for [elem elements
                                      :let [fact (:fact elem)
                                            fb   (:bindings elem)
                                            mp   (match-pair fact node-id)]
                                      token removed]
                                     (->Token (conj (:matches token) mp)
                                              (conj (:bindings token) fb)))
                 (let [token (first removed)]
                   (into [] (map (fn [elem]
                                   (->Token (conj (:matches token)
                                                  (match-pair (:fact elem) node-id))
                                            (conj (:bindings token) (:bindings elem)))))
                         elements)))))))))))

;; ---------------------------------------------------------------------------
;; Helper: generic evaluate-delta that delegates to the node's existing
;; ILeftActivate / IRightActivate methods (which already use transport for
;; downstream propagation).  Works correctly for any node type whose
;; right-activate/left-activate methods do NOT contain demand-pull logic.

(defn- default-evaluate-delta
  "Generic evaluate-delta: drain element/token ops (in arrival order) and call
   the node's existing right-activate, right-retract, left-activate, left-retract
   methods.  Element ops are processed before token ops so element memory is
   up to date when token joins run."
  [node memory delta-memory transport listener]
  (let [node-id (:id node)]
    (doseq [[insert? join-bindings elems] (dm/get-element-ops delta-memory node-id)]
      (if insert?
        (right-activate node join-bindings elems memory transport listener)
        (right-retract  node join-bindings elems memory transport listener)))
    (doseq [[insert? join-bindings toks] (dm/get-token-ops delta-memory node-id)]
      (if insert?
        (left-activate node join-bindings toks memory transport listener)
        (left-retract  node join-bindings toks memory transport listener)))))

;; All remaining node types use the generic delegate:

(extend-type ExpressionJoinNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

(extend-type TestNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

(extend-type NegationNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

(extend-type NegationWithJoinFilterNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

(extend-type AccumulateNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

(extend-type AccumulateWithJoinFilterNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

(extend-type ProductionNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

(extend-type QueryNode
  IDeltaEvaluate
  (evaluate-delta [node memory delta-memory transport listener]
    (default-evaluate-delta node memory delta-memory transport listener)))

;; ---------------------------------------------------------------------------
;; Pull-phase helpers

(defn- max-downstream-salience
  "Returns the highest salience found among all ProductionNodes reachable
   from `node`.  Used to sort beta-roots so high-salience rules generate
   activations earlier in the pull phase."
  [node]
  (let [visited (volatile! #{})
        result  (volatile! 0)]
    (letfn [(visit! [n]
              (when-not (contains? @visited (:id n))
                (vswap! visited conj (:id n))
                (when (instance? ProductionNode n)
                  (let [s (get-in n [:production :props :salience] 0)]
                    (when (> s @result) (vreset! result s))))
                (doseq [child (:children n)] (visit! child))))]
      (visit! node))
    @result))

(defn- compute-beta-topo-order
  "DFS pre-order traversal of the beta network from beta-roots, sorted by
   descending max downstream salience so high-salience rules generate
   activations earlier in the pull phase.
   For tree topologies (the common case) this is a strict topo sort.
   For DAGs a node may appear once; if a second parent is processed later
   the node will be marked dirty again and re-evaluated in the next pass of
   the pull-phase loop — the loop converges to a quiescent state."
  [beta-roots]
  (let [visited      (volatile! #{})
        result       (volatile! (transient []))
        sorted-roots (sort-by #(- (max-downstream-salience %)) beta-roots)]
    (letfn [(visit! [node]
              (when-not (contains? @visited (:id node))
                (vswap! visited conj (:id node))
                (vswap! result  conj! node)
                (doseq [child (:children node)]
                  (visit! child))))]
      (doseq [root sorted-roots]
        (visit! root)))
    (persistent! @result)))

(defn- run-pull-phase!
  "Evaluate every dirty node in topological order, then repeat until quiescent.
   Using DFS pre-order topo-order means children are always processed after
   their parents, so token deltas flow naturally in a single pass for trees.
   The outer loop handles the rare DAG case where a node is dirtied again
   after it has already been processed in the current pass."
  [beta-topo-order memory delta-memory transport listener]
  (loop []
    (when (dm/any-dirty? delta-memory)
      (doseq [node beta-topo-order
              :when (dm/dirty? delta-memory (:id node))]
        (evaluate-delta node memory delta-memory transport listener)
        (dm/clear-dirty! delta-memory (:id node)))
      (recur))))

;; ---------------------------------------------------------------------------
;; PHREAK fire-rules* — pull-phase loop + rule firing

(defn- fire-rules-phreak*
  "PHREAK-style rule firing: after each batch of alpha activations the pull
   phase evaluates dirty nodes top-down, then the fire loop fires any
   activations that were produced, processes inserts/retracts from the RHS,
   re-runs the pull phase, and continues until quiescent."
  [rulebase transient-memory phreak-transport transient-listener
   get-alphas-fn update-cache beta-topo-order delta-memory]
  (let [batched-logical-insertions      (volatile! [])
        batched-unconditional-insertions (volatile! [])
        batched-rhs-retractions          (volatile! [])
        rule-context                     (volatile! nil)
        null-listener                    (l/null-listener? transient-listener)]

    (binding [*current-session* {:rulebase         rulebase
                                 :transient-memory transient-memory
                                 :transport        phreak-transport
                                 :insertions       (volatile! 0)
                                 :get-alphas-fn    get-alphas-fn
                                 :pending-updates  update-cache
                                 :listener         transient-listener
                                 :rule-context     rule-context}]

      (letfn [(do-flush-and-pull []
                ;; Flush any pending updates (from RHS inserts/retracts or
                ;; logical retractions), then re-run the pull phase.
                ;; Repeat until nothing new arrives.
                (loop []
                  (run-pull-phase! beta-topo-order transient-memory delta-memory
                                   phreak-transport transient-listener)
                  (when (flush-updates *current-session*)
                    (recur))))]

        ;; Initial pull phase to process external insertions/retractions that
        ;; were queued before fire-rules-phreak* was called.
        (do-flush-and-pull)

        (loop [next-group (mem/next-activation-group transient-memory)
               last-group nil]

          (if next-group

            (if (and last-group (not= last-group next-group))
              ;; Group boundary — flush and pull before continuing.
              (do
                (do-flush-and-pull)
                (let [upcoming-group (mem/next-activation-group transient-memory)]
                  (l/activation-group-transition! transient-listener next-group upcoming-group)
                  (recur upcoming-group next-group)))

              (do
                ;; Fire one activation if available.
                (when-let [{:keys [node token] :as activation}
                           (mem/pop-activation! transient-memory)]
                  (vreset! batched-logical-insertions      [])
                  (vreset! batched-unconditional-insertions [])
                  (vreset! batched-rhs-retractions          [])
                  (vreset! rule-context
                           (->RuleContext token node
                                         batched-logical-insertions
                                         batched-unconditional-insertions
                                         batched-rhs-retractions))

                  (try
                    ((:rhs node) token (:env (:production node)))
                    (let [retrieved-unconditional @batched-unconditional-insertions
                          retrieved-logical       @batched-logical-insertions
                          retrieved-retractions   @batched-rhs-retractions]
                      (when-not null-listener
                        (l/fire-activation! transient-listener activation
                                            {:unconditional-insertions retrieved-unconditional
                                             :logical-insertions       retrieved-logical
                                             :rhs-retractions          retrieved-retractions}))
                      (when-let [batched (seq retrieved-unconditional)]
                        (flush-insertions! batched true))
                      (when-let [batched (seq retrieved-logical)]
                        (flush-insertions! batched false))
                      (when-let [batched (seq retrieved-retractions)]
                        (flush-rhs-retractions! batched)))
                    (catch #?(:clj Exception :cljs :default) e
                      (let [production (:production node)
                            rule-name  (:name production)
                            rhs        (:rhs production)]
                        (throw (ex-info (str "Exception in "
                                             (if rule-name rule-name (pr-str rhs))
                                             " with bindings "
                                             (pr-str (:bindings token)))
                                        {:bindings (:bindings token)
                                         :name     rule-name
                                         :rhs      rhs
                                         :batched-logical-insertions      @batched-logical-insertions
                                         :batched-unconditional-insertions @batched-unconditional-insertions
                                         :batched-rhs-retractions          @batched-rhs-retractions
                                         :listeners
                                         (try
                                           (let [p-listener (l/to-persistent! transient-listener)]
                                             (if (l/null-listener? p-listener)
                                               []
                                               (l/get-children p-listener)))
                                           (catch #?(:clj Exception :cljs :default)
                                               listener-exception
                                             listener-exception))}
                                        e)))))

                  (when (some-> node :production :props :no-loop)
                    (flush-updates *current-session*)))

                (recur (mem/next-activation-group transient-memory) next-group)))

            ;; No activations — flush pending updates and pull; if new
            ;; activations appeared, loop again.
            (do
              (do-flush-and-pull)
              (let [upcoming-group (mem/next-activation-group transient-memory)]
                (when upcoming-group
                  (l/activation-group-transition! transient-listener next-group upcoming-group)
                  (recur upcoming-group next-group))))))))))

;;; End PHREAK section
;;; ============================================================

(defn fire-rules*
  "Fire rules for the given nodes."
  [rulebase _nodes transient-memory transport listener get-alphas-fn update-cache]
  ;; Hoist per-activation volatiles before the loop to avoid re-allocating them on every activation.
  ;; They are vreset! to [] before each RHS call.
  (let [batched-logical-insertions (volatile! [])
        batched-unconditional-insertions (volatile! [])
        batched-rhs-retractions (volatile! [])
        rule-context (volatile! nil)
        null-listener (l/null-listener? listener)]
    (binding [*current-session* {:rulebase rulebase
                                 :transient-memory transient-memory
                                 :transport transport
                                 :insertions (volatile! 0)
                                 :get-alphas-fn get-alphas-fn
                                 :pending-updates update-cache
                                 :listener listener
                                 :rule-context rule-context}]

      (loop [next-group (mem/next-activation-group transient-memory)
             last-group nil]

        (if next-group

          (if (and last-group (not= last-group next-group))

            ;; We have changed groups, so flush the updates from the previous
            ;; group before continuing.
            (do
              (flush-updates *current-session*)
              (let [upcoming-group (mem/next-activation-group transient-memory)]
                (l/activation-group-transition! listener next-group upcoming-group)
                (recur upcoming-group next-group)))

            (do

              ;; If there are activations, fire them.
              (when-let [{:keys [node token] :as activation} (mem/pop-activation! transient-memory)]
                ;; Use vectors for the insertion caches so that within an insertion type
                ;; (unconditional or logical) all insertions are done in order after the into
                ;; calls in insert-facts!.  This shouldn't have a functional impact, since any ordering
                ;; should be valid, but makes traces less confusing to end users.  It also prevents any laziness
                ;; in the sequences.
                (vreset! batched-logical-insertions [])
                (vreset! batched-unconditional-insertions [])
                (vreset! batched-rhs-retractions [])
                (vreset! rule-context (->RuleContext token node
                                                     batched-logical-insertions
                                                     batched-unconditional-insertions
                                                     batched-rhs-retractions))

                ;; Fire the rule itself.
                (try
                  ((:rhs node) token (:env (:production node)))
                  ;; Don't do anything if a given insertion type has no corresponding
                  ;; facts to avoid complicating traces.  Note that since each no RHS's of
                  ;; downstream rules are fired here everything is governed by truth maintenance.
                  ;; Therefore, the reordering of retractions and insertions should have no impact
                  ;; assuming that the evaluation of rule conditions is pure, which is a general expectation
                  ;; of the rules engine.
                  ;;
                  ;; Bind the contents of the cache atoms after the RHS is fired since they are used twice
                  ;; below.  They will be dereferenced again if an exception is caught, but in the error
                  ;; case we aren't worried about performance.
                  (let [retrieved-unconditional-insertions @batched-unconditional-insertions
                        retrieved-logical-insertions @batched-logical-insertions
                        retrieved-rhs-retractions @batched-rhs-retractions]
                    (when-not null-listener
                      (l/fire-activation! listener
                                          activation
                                          {:unconditional-insertions retrieved-unconditional-insertions
                                           :logical-insertions retrieved-logical-insertions
                                           :rhs-retractions retrieved-rhs-retractions}))
                    (when-let [batched (seq retrieved-unconditional-insertions)]
                      (flush-insertions! batched true))
                    (when-let [batched (seq retrieved-logical-insertions)]
                      (flush-insertions! batched false))
                    (when-let [batched (seq retrieved-rhs-retractions)]
                      (flush-rhs-retractions! batched)))
                  (catch #?(:clj Exception :cljs :default) e

                         ;; If the rule fired an exception, help debugging by attaching
                         ;; details about the rule itself, cached insertions, and any listeners
                         ;; while propagating the cause.
                         (let [production (:production node)
                               rule-name (:name production)
                               rhs (:rhs production)]
                           (throw (ex-info (str "Exception in " (if rule-name rule-name (pr-str rhs))
                                                " with bindings " (pr-str (:bindings token)))
                                           {:bindings (:bindings token)
                                            :name rule-name
                                            :rhs rhs
                                            :batched-logical-insertions @batched-logical-insertions
                                            :batched-unconditional-insertions @batched-unconditional-insertions
                                            :batched-rhs-retractions @batched-rhs-retractions
                                            :listeners (try
                                                         (let [p-listener (l/to-persistent! listener)]
                                                           (if (l/null-listener? p-listener)
                                                             []
                                                             (l/get-children p-listener)))
                                                         (catch #?(:clj Exception :cljs :default)
                                                             listener-exception
                                                           listener-exception))}
                                           e)))))

                ;; Explicitly flush updates if we are in a no-loop rule, so the no-loop
                ;; will be in context for child rules.
                (when (some-> node :production :props :no-loop)
                  (flush-updates *current-session*)))

              (recur (mem/next-activation-group transient-memory) next-group)))

          ;; There were no items to be activated, so flush any pending
          ;; updates and recur with a potential new activation group
          ;; since a flushed item may have triggered one.
          (when (flush-updates *current-session*)
            (let [upcoming-group (mem/next-activation-group transient-memory)]
              (l/activation-group-transition! listener next-group upcoming-group)
              (recur upcoming-group next-group))))))))

(deftype LocalSession [rulebase memory transport listener get-alphas-fn pending-operations]
  ISession
  (insert [_session facts]

    (let [new-pending-operations (conj pending-operations (uc/->PendingUpdate :insertion
                                                                              ;; Preserve the behavior prior to https://github.com/cerner/clara-rules/issues/268
                                                                              ;; , particularly for the Java API, where the caller could freely mutate a
                                                                              ;; collection of facts after passing it to Clara for the constituent
                                                                              ;; facts to be inserted or retracted.  If the caller passes a persistent
                                                                              ;; Clojure collection don't do any additional work.
                                                                              (if (coll? facts)
                                                                                facts
                                                                                (into [] facts))))]

      (LocalSession. rulebase
                     memory
                     transport
                     listener
                     get-alphas-fn
                     new-pending-operations)))

  (retract [_session facts]

    (let [new-pending-operations (conj pending-operations (uc/->PendingUpdate :retraction
                                                                              ;; As in insert above defend against facts being a mutable collection.
                                                                              (if (coll? facts)
                                                                                facts
                                                                                (into [] facts))))]

      (LocalSession. rulebase
                     memory
                     transport
                     listener
                     get-alphas-fn
                     new-pending-operations)))

  ;; Prior to issue 249 we only had a one-argument fire-rules method.  clara.rules/fire-rules will always call the two-argument method now
  ;; but we kept a one-argument version of the fire-rules in case anyone is calling the fire-rules protocol function or method on the session directly.
  (fire-rules [session] (fire-rules session {}))
  (fire-rules [session opts]

    ;; Merge in any dynamically-bound extra opts (e.g., {:use-phreak true}
    ;; injected by opts-fixture for integration testing).
    (let [opts (merge *additional-fire-rules-opts* opts)
          transient-memory (mem/to-transient memory)
          transient-listener (l/to-transient listener)]

      (cond
        ;; ── PHREAK path ────────────────────────────────────────────────────
        ;; Delta queues + pull phase instead of immediate eager cascade.
        ;; Gate behind {:use-phreak true} AND only when :cancelling is not
        ;; explicitly requested (cancelling semantics override PHREAK).
        (and (:use-phreak opts) (not (:cancelling opts)))
        (let [delta-memory     (dm/make-transient-delta-memory)
              phreak-transport (->DeltaTransport delta-memory)
              beta-topo-order  (compute-beta-topo-order (:beta-roots rulebase))
              update-cache     (uc/get-ordered-update-cache)]
          ;; Route external ops through DeltaTransport — this only queues
          ;; element deltas and marks nodes dirty; no immediate cascade.
          ;; Truth-maintenance cascades arising from these insertions are
          ;; handled by the pull phase inside fire-rules-phreak*.
          (doseq [{op-type :type facts :facts} pending-operations]
            (case op-type
              :insertion
              (do
                (l/insert-facts! transient-listener nil nil facts)
                (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
                        root alpha-roots]
                  (alpha-activate root fact-group transient-memory phreak-transport transient-listener)))
              :retraction
              (do
                (l/retract-facts! transient-listener nil nil facts)
                (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
                        root alpha-roots]
                  (alpha-retract root fact-group transient-memory phreak-transport transient-listener)))))
          (fire-rules-phreak* rulebase transient-memory phreak-transport transient-listener
                              get-alphas-fn update-cache beta-topo-order delta-memory))

        ;; ── Standard eager path ────────────────────────────────────────────
        (not (:cancelling opts))
        ;; We originally performed insertions and retractions immediately after the insert and retract calls,
        ;; but this had the downside of making a pattern like "Retract facts, insert other facts, and fire the rules"
        ;; perform at least three transitions between a persistent and transient memory.  Delaying the actual execution
        ;; of the insertions and retractions until firing the rules allows us to cut this down to a single transition
        ;; between persistent and transient memory.  There is some cost to the runtime dispatch on operation types here,
        ;; but this is presumably less significant than the cost of memory transitions.
        ;;
        ;; We perform the insertions and retractions in the same order as they were applied to the session since
        ;; if a fact is not in the session, retracted, and then subsequently inserted it should be in the session at
        ;; the end.
        (do
          (doseq [{op-type :type facts :facts} pending-operations]

            (case op-type

              :insertion
              (do
                (l/insert-facts! transient-listener nil nil facts)

                (binding [*pending-external-retractions* (volatile! [])]
                  ;; Bind the external retractions cache so that any logical retractions as a result
                  ;; of these insertions can be cached and executed as a batch instead of eagerly realizing
                  ;; them.  An external insertion of a fact that matches
                  ;; a negation or accumulator condition can cause logical retractions.
                  (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
                          root alpha-roots]
                    (alpha-activate root fact-group transient-memory transport transient-listener))
                  (external-retract-loop get-alphas-fn transient-memory transport transient-listener)))

              :retraction
              (do
                (l/retract-facts! transient-listener nil nil facts)

                (binding [*pending-external-retractions* (volatile! facts)]
                  (external-retract-loop get-alphas-fn transient-memory transport transient-listener)))))

          (fire-rules* rulebase
                       (:production-nodes rulebase)
                       transient-memory
                       transport
                       transient-listener
                       get-alphas-fn
                       (uc/get-ordered-update-cache)))

        ;; ── Cancelling (CAS-based) path ────────────────────────────────────
        :else
        #?(:cljs (throw (ex-info "The :cancelling option is not supported in ClojureScript"
                                 {:session session :opts opts}))

           :clj (let [_ session
                      insertions (sequence
                                  (comp (filter (fn [pending-op]
                                                  (= (:type pending-op)
                                                     :insertion)))
                                        (mapcat :facts))
                                  pending-operations)

                      retractions (sequence
                                   (comp (filter (fn [pending-op]
                                                   (= (:type pending-op)
                                                      :retraction)))
                                         (mapcat :facts))
                                   pending-operations)

                      update-cache (ca/get-cancelling-update-cache)]

                  (binding [*current-session* {:rulebase rulebase
                                               :transient-memory transient-memory
                                               :transport transport
                                               :insertions (volatile! 0)
                                               :get-alphas-fn get-alphas-fn
                                               :pending-updates update-cache
                                               :listener transient-listener}]

                    ;; Insertions should come before retractions so that if we insert and then retract the same
                    ;; fact that is not already in the session the end result will be that the session won't have that fact.
                    ;; If retractions came first then we'd first retract a fact that isn't in the session, which doesn't do anything,
                    ;; and then later we would insert the fact.
                    (doseq [[alpha-roots fact-group] (get-alphas-fn insertions)
                            root alpha-roots]
                      (alpha-activate root fact-group transient-memory transport transient-listener))

                    (doseq [[alpha-roots fact-group] (get-alphas-fn retractions)
                            root alpha-roots]
                      (alpha-retract root fact-group transient-memory transport transient-listener))

                    (fire-rules* rulebase
                                 (:production-nodes rulebase)
                                 transient-memory
                                 transport
                                 transient-listener
                                 get-alphas-fn
                                 ;; This continues to use the cancelling cache after the first batch of insertions and retractions.
                                 ;; If this is suboptimal for some workflows we can revisit this.
                                 update-cache)))))

      (LocalSession. rulebase
                     (mem/to-persistent! transient-memory)
                     transport
                     (l/to-persistent! transient-listener)
                     get-alphas-fn
                     [])))

  (query [_session query params]
    (let [query-node (get-in rulebase [:query-nodes query])]
      (when (= nil query-node)
        (platform/throw-error (str "The query " query " is invalid or not included in the rule base.")))
      (when-not (= (into #{} (keys params)) ;; nil params should be equivalent to #{}
                   (:param-keys query-node))
        (platform/throw-error (str "The query " query " was not provided with the correct parameters, expected: "
                                   (:param-keys query-node) ", provided: " (set (keys params)))))

      (->> (mem/get-tokens memory query-node params)

           ;; Get the bindings for each token and filter generate symbols.
           (map (fn [{bindings :bindings}]

                  ;; Filter generated symbols. We check first since this is an uncommon flow.
                  (if (some #(re-find #"__gen" (name %)) (keys bindings))

                    (into {} (remove (fn [[k _v]] (re-find #"__gen"  (name k)))
                                     bindings))
                    bindings))))))

  (components [_session]
    {:rulebase rulebase
     :memory memory
     :transport transport
     :listeners (l/flatten-listener listener)
     :get-alphas-fn get-alphas-fn}))

(defn assemble
  "Assembles a session from the given components, which must be a map
   containing the following:

   :rulebase A recorec matching the clara.rules.compiler/Rulebase structure.
   :memory An implementation of the clara.rules.memory/IMemoryReader protocol
   :transport An implementation of the clara.rules.engine/ITransport protocol
   :listeners A vector of listeners implementing the clara.rules.listener/IPersistentListener protocol
   :get-alphas-fn The function used to return the alpha nodes for a fact of the given type."

  [{:keys [rulebase memory transport listeners get-alphas-fn]}]
  (LocalSession. rulebase
                 memory
                 transport
                 (if (> (count listeners) 0)
                   (l/delegating-listener listeners)
                   l/default-listener)
                 get-alphas-fn
                 []))

(defn- throw-unsupported-read-only-operation
  "Throws an exception indicating that a mutating operation is not supported on a read-only session."
  [op-name]
  (throw #?(:clj (UnsupportedOperationException.
                   (str op-name " is not supported on a read-only session."))
             :cljs (js/Error.
                     (str op-name " is not supported on a read-only session.")))))

(deftype ReadOnlyLocalSession [rulebase memory transport listener get-alphas-fn]
  ISession
  (insert [_session _facts]
    (throw-unsupported-read-only-operation "insert"))

  (retract [_session _facts]
    (throw-unsupported-read-only-operation "retract"))

  (fire-rules [_session]
    (throw-unsupported-read-only-operation "fire-rules"))

  (fire-rules [_session _opts]
    (throw-unsupported-read-only-operation "fire-rules"))

  (query [_session query params]
    (let [query-node (get-in rulebase [:query-nodes query])]
      (when (= nil query-node)
        (platform/throw-error (str "The query " query " is invalid or not included in the rule base.")))
      (when-not (= (into #{} (keys params))
                   (:param-keys query-node))
        (platform/throw-error (str "The query " query " was not provided with the correct parameters, expected: "
                                   (:param-keys query-node) ", provided: " (set (keys params)))))

      (->> (mem/get-tokens memory query-node params)
           (map (fn [{bindings :bindings}]
                  (if (some #(re-find #"__gen" (name %)) (keys bindings))
                    (into {} (remove (fn [[k _v]] (re-find #"__gen" (name k)))
                                     bindings))
                    bindings))))))

  (components [_session]
    {:rulebase rulebase
     :memory memory
     :transport transport
     :listeners (l/flatten-listener listener)
     :get-alphas-fn get-alphas-fn}))

(defn assemble-read-only
  "Assembles a read-only session from the given components. The resulting session supports
   query and components but throws on insert, retract, and fire-rules."
  [{:keys [rulebase memory transport listeners get-alphas-fn]}]
  (ReadOnlyLocalSession. rulebase
                         memory
                         transport
                         (if (> (count listeners) 0)
                           (l/delegating-listener listeners)
                           l/default-listener)
                         get-alphas-fn))

(defn as-read-only
  "Returns a read-only version of the given session that supports query and components
   but throws on insert, retract, and fire-rules."
  [session]
  (assemble-read-only (components session)))

(defn with-listener
  "Return a new session with the listener added to the provided session,
   in addition to all listeners previously on the session."
  [session listener]
  (let [{:keys [listeners] :as components} (components session)]
    (assemble (assoc components
                     :listeners
                     (conj listeners
                           listener)))))

(defn remove-listeners
  "Return a new session with all listeners matching the predicate removed"
  [session pred]
  (let [{:keys [listeners] :as components} (components session)]
    (if (some pred listeners)
      (assemble (assoc components
                       :listeners
                       (into [] (remove pred) listeners)))
      session)))

(defn find-listeners
  "Return all listeners on the session matching the predicate."
  [session pred]
  (let [{:keys [listeners]} (components session)]
    (filterv pred listeners)))

(defn local-memory
  "Returns a local, in-process working memory."
  [rulebase transport activation-group-sort-fn activation-group-fn alphas-fn]
  (let [memory (mem/to-transient (mem/local-memory rulebase activation-group-sort-fn activation-group-fn alphas-fn))]
    (doseq [beta-node (:beta-roots rulebase)]
      (left-activate beta-node {} [empty-token] memory transport l/default-listener))
    (mem/to-persistent! memory)))

(defn options->activation-group-sort-fn
  "Given the map of options for a session, construct an activation group sorting
  function that takes into account the user-provided salience and internal salience.
  User-provided salience is considered first.  Under normal circumstances this function should
  only be called by Clara itself."
  [options]
  (let [user-activation-group-sort-fn (or (get options :activation-group-sort-fn)
                                          ;; Default to sort by descending numerical order.
                                          >)]

    ;; Compare user-provided salience first, using either the provided salience function or the default,
    ;; then use the internal salience if the former does not provide an ordering between the two salience values.
    (fn [salience1 salience2]
      (let [forward-result (user-activation-group-sort-fn (nth salience1 0)
                                                          (nth salience2 0))]
        (if (number? forward-result)
          (if (= 0 forward-result)
            (> (nth salience1 1)
               (nth salience2 1))

            forward-result)
          (let [backward-result (user-activation-group-sort-fn (nth salience2 0)
                                                               (nth salience1 0))
                forward-bool (boolean forward-result)
                backward-bool (boolean backward-result)]
            ;; Since we just use Clojure functions, for example >, equality may be implied
            ;; by returning false for comparisons in both directions rather than by returning 0.
            ;; Furthermore, ClojureScript will use truthiness semantics rather than requiring a
            ;; boolean (unlike Clojure), so we use the most permissive semantics between Clojure
            ;; and ClojureScript.
            (if (not= forward-bool backward-bool)
              forward-bool
              (> (nth salience1 1)
                 (nth salience2 1)))))))))

(def ^:private internal-salience-levels {:default 0
                                         ;; Extracted negations need to be prioritized over their original
                                         ;; rules since their original rule could fire before the extracted condition.
                                         ;; This is a problem if the original rule performs an unconditional insertion
                                         ;; or has other side effects not controlled by truth maintenance.
                                         :extracted-negation 1})

(defn options->activation-group-fn
  "Given a map of options for a session, construct a function that takes a production
  and returns the activation group to which it belongs, considering both user-provided
  and internal salience.  Under normal circumstances this function should only be called by
  Clara itself."
  [options]
  (let [rule-salience-fn (or (:activation-group-fn options)
                             (fn [production] (or (some-> production :props :salience)
                                                  0)))]

    (fn [production]
      [(rule-salience-fn production)
       (internal-salience-levels (or (some-> production :props :clara-rules/internal-salience)
                                     :default))])))

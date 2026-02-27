(ns clara.rules.delta-memory
  "Delta memory for PHREAK-style pull-phase evaluation.

   Tracks pending element/token operations per (node-id, join-bindings) pair in
   INSERTION ORDER, plus a dirty-node set so the pull phase only visits nodes
   with pending work.

   Ordering is critical: a session-level (retract f)(insert f) sequence must
   result in f present in memory, while (insert f)(retract f) must result in f
   absent.  Both orderings collapse to the same unordered insert+retract pair
   if we use separate insert/retract maps, so we preserve the original order as
   an ordered list of [insert? join-bindings elements] triples.

   Mutable backing stores:
     CLJ  — java.util.HashMap (node-id → java.util.ArrayList of op tuples)
              + java.util.HashSet for the dirty set.
     CLJS — goog.object map (node-id-str → persistent vector of op tuples)
              + js/Set for the dirty set.
   Op tuples remain Clojure vectors so consume sites can use
   (doseq [[insert? join-bindings elems] ops] ...) without change."
  #?(:cljs (:require [goog.object :as gobject]))
  #?(:clj  (:import [java.util ArrayList HashMap HashSet])))

(defprotocol IDeltaMemory
  "Mutable accumulator for incremental (delta) changes during the PHREAK pull phase."

  (add-element-op! [dm node-id join-bindings elements insert?]
    "Append an element insert (insert?=true) or retract (insert?=false) operation
     for node-id.  Operations are stored in arrival order so evaluate-delta can
     replay them in the correct sequence.")

  (get-element-ops [dm node-id]
    "Return [[insert? join-bindings [Element]]] for node-id in arrival order,
     or [] when there are no pending element operations.")

  (add-token-op! [dm node-id join-bindings tokens insert?]
    "Append a token insert or retract operation for node-id.")

  (get-token-ops [dm node-id]
    "Return [[insert? join-bindings [Token]]] for node-id in arrival order,
     or [] when there are no pending token operations.")

  (dirty? [dm node-id]
    "Returns true if node-id has pending operations that have not yet been evaluated.")

  (mark-dirty! [dm node-id]
    "Mark node-id as having pending operations.")

  (clear-dirty! [dm node-id]
    "Clear node-id from the dirty set and remove all queued operations for it.
     Called after evaluate-delta has consumed the node's pending work.")

  (any-dirty? [dm]
    "Returns true if any node has pending operations.")

  (get-dirty-nodes [dm]
    "Returns the set of dirty node IDs (a snapshot; not guaranteed to be sorted)."))

;; ---------------------------------------------------------------------------
;; TransientDeltaMemory

(deftype TransientDeltaMemory
  [elem-ops   ;; CLJ: HashMap<node-id, ArrayList<op-tuple>>  CLJS: goog.object map
   tok-ops    ;; CLJ: HashMap<node-id, ArrayList<op-tuple>>  CLJS: goog.object map
   dirty]     ;; CLJ: HashSet<node-id>                       CLJS: js/Set

  IDeltaMemory

  (add-element-op! [_ node-id join-bindings elements insert?]
    #?(:clj  (let [^HashMap m elem-ops
                   ^ArrayList al (or (.get m node-id)
                                     (let [a (ArrayList.)]
                                       (.put m node-id a)
                                       a))]
               (.add al [insert? join-bindings elements]))
       :cljs (let [k (str node-id)]
               (gobject/set elem-ops k
                            (conj (or (gobject/get elem-ops k) [])
                                  [insert? join-bindings elements])))))

  (get-element-ops [_ node-id]
    #?(:clj  (let [^ArrayList al (.get ^HashMap elem-ops node-id)]
               (if al (seq al) []))
       :cljs (or (gobject/get elem-ops (str node-id)) [])))

  (add-token-op! [_ node-id join-bindings tokens insert?]
    #?(:clj  (let [^HashMap m tok-ops
                   ^ArrayList al (or (.get m node-id)
                                     (let [a (ArrayList.)]
                                       (.put m node-id a)
                                       a))]
               (.add al [insert? join-bindings tokens]))
       :cljs (let [k (str node-id)]
               (gobject/set tok-ops k
                            (conj (or (gobject/get tok-ops k) [])
                                  [insert? join-bindings tokens])))))

  (get-token-ops [_ node-id]
    #?(:clj  (let [^ArrayList al (.get ^HashMap tok-ops node-id)]
               (if al (seq al) []))
       :cljs (or (gobject/get tok-ops (str node-id)) [])))

  (dirty? [_ node-id]
    #?(:clj  (.contains ^HashSet dirty node-id)
       :cljs (.has ^js dirty node-id)))

  (mark-dirty! [_ node-id]
    #?(:clj  (.add ^HashSet dirty node-id)
       :cljs (.add ^js dirty node-id)))

  (clear-dirty! [_ node-id]
    #?(:clj  (do (.remove ^HashSet dirty node-id)
                 (.remove ^HashMap elem-ops node-id)
                 (.remove ^HashMap tok-ops node-id))
       :cljs (do (.delete ^js dirty node-id)
                 (gobject/remove elem-ops (str node-id))
                 (gobject/remove tok-ops (str node-id)))))

  (any-dirty? [_]
    #?(:clj  (not (.isEmpty ^HashSet dirty))
       :cljs (pos? (.-size ^js dirty))))

  (get-dirty-nodes [_]
    #?(:clj  (set dirty)
       :cljs (into #{} (.values ^js dirty)))))

(defn make-transient-delta-memory
  "Construct a fresh TransientDeltaMemory backed by mutable Java/JS collections."
  []
  #?(:clj  (->TransientDeltaMemory (HashMap.) (HashMap.) (HashSet.))
     :cljs (->TransientDeltaMemory (js-obj) (js-obj) (js/Set.))))

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

   Structure of each mutable field:
     elem-ops  volatile! {node-id -> [[insert? join-bindings [Element]]]}
     tok-ops   volatile! {node-id -> [[insert? join-bindings [Token]]]}
     dirty     volatile! #{node-id}")

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
  [elem-ops   ;; volatile! {node-id -> [[insert? join-bindings [Element]]]}
   tok-ops    ;; volatile! {node-id -> [[insert? join-bindings [Token]]]}
   dirty]     ;; volatile! #{node-id}

  IDeltaMemory

  (add-element-op! [_ node-id join-bindings elements insert?]
    (vswap! elem-ops update node-id
            (fn [ops] (conj (or ops []) [insert? join-bindings elements]))))

  (get-element-ops [_ node-id]
    (or (get @elem-ops node-id) []))

  (add-token-op! [_ node-id join-bindings tokens insert?]
    (vswap! tok-ops update node-id
            (fn [ops] (conj (or ops []) [insert? join-bindings tokens]))))

  (get-token-ops [_ node-id]
    (or (get @tok-ops node-id) []))

  (dirty? [_ node-id]
    (contains? @dirty node-id))

  (mark-dirty! [_ node-id]
    (vswap! dirty conj node-id))

  (clear-dirty! [_ node-id]
    (vswap! dirty disj node-id)
    (vswap! elem-ops dissoc node-id)
    (vswap! tok-ops  dissoc node-id))

  (any-dirty? [_]
    (seq @dirty))

  (get-dirty-nodes [_]
    @dirty))

(defn make-transient-delta-memory
  "Construct a fresh TransientDeltaMemory."
  []
  (->TransientDeltaMemory (volatile! {})
                          (volatile! {})
                          (volatile! #{})))

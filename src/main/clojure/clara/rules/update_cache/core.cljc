(ns clara.rules.update-cache.core
  #?(:clj (:import [java.util ArrayList Collections])))

;; Record indicating pending insertion or removal of a sequence of facts.
(defrecord PendingUpdate [type facts])

;; This is expected to be used while activating rules in a given salience group
;; to store updates before propagating those updates to the alpha nodes as a group.
(defprotocol UpdateCache
  (add-insertions! [this facts])
  (add-retractions! [this facts])
  (get-updates-and-reset! [this]))

;; This cache replicates the behavior prior to https://github.com/cerner/clara-rules/issues/249,
;; just in a stateful object rather than a persistent data structure.
;; Uses mutable collections to avoid atom CAS overhead during single-threaded fire-rules*.
#?(:clj
   (deftype OrderedUpdateCache [^:unsynchronized-mutable ^ArrayList updates]

     UpdateCache

     (add-insertions! [this facts]
       (.add updates (->PendingUpdate :insert facts)))

     (add-retractions! [this facts]
       (.add updates (->PendingUpdate :retract facts)))

     (get-updates-and-reset! [this]
       (let [current-updates (Collections/unmodifiableList updates)]
         (set! updates (ArrayList.))
         (partition-by :type current-updates))))

   :cljs
   (deftype OrderedUpdateCache [^:mutable updates]

     UpdateCache

     (add-insertions! [this facts]
       (set! updates (conj updates (->PendingUpdate :insert facts))))

     (add-retractions! [this facts]
       (set! updates (conj updates (->PendingUpdate :retract facts))))

     (get-updates-and-reset! [this]
       (let [current-updates updates]
         (set! updates [])
         (partition-by :type current-updates)))))

(defn get-ordered-update-cache
  []
  #?(:clj (OrderedUpdateCache. (ArrayList.))
     :cljs (OrderedUpdateCache. [])))

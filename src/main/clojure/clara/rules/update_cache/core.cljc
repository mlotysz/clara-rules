(ns clara.rules.update-cache.core
  #?(:clj (:import [java.util ArrayList])))

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
;;
;; get-updates-and-reset! returns a vector of [type facts-vec] pairs where consecutive
;; PendingUpdates of the same type have their :facts concatenated eagerly.
#?(:clj
   (deftype OrderedUpdateCache [^:unsynchronized-mutable ^ArrayList updates]

     UpdateCache

     (add-insertions! [this facts]
       (.add updates (->PendingUpdate :insert facts)))

     (add-retractions! [this facts]
       (.add updates (->PendingUpdate :retract facts)))

     (get-updates-and-reset! [this]
       (let [n (.size updates)]
         (if (zero? n)
           []
           (let [result (ArrayList.)]
             (loop [i (int 0)]
               (when (< i n)
                 (let [^PendingUpdate pu (.get updates i)
                       current-type (.-type pu)
                       ;; Scan forward to find the end of this consecutive run of same-type updates
                       end (loop [j (unchecked-inc-int i)]
                             (if (and (< j n)
                                      (identical? current-type (.-type ^PendingUpdate (.get updates j))))
                               (recur (unchecked-inc-int j))
                               j))]
                   (if (= (unchecked-inc-int i) end)
                     ;; Single update in this run — use its facts directly
                     (.add result [current-type (.-facts pu)])
                     ;; Multiple consecutive updates — concat their facts
                     (let [facts (loop [j (unchecked-inc-int i)
                                        acc (transient (vec (.-facts pu)))]
                                   (if (< j end)
                                     (recur (unchecked-inc-int j)
                                            (reduce conj! acc (.-facts ^PendingUpdate (.get updates j))))
                                     (persistent! acc)))]
                       (.add result [current-type facts])))
                   (recur end))))
             (set! updates (ArrayList.))
             (vec result))))))

   :cljs
   (deftype OrderedUpdateCache [^:mutable updates]

     UpdateCache

     (add-insertions! [this facts]
       (set! updates (conj updates (->PendingUpdate :insert facts))))

     (add-retractions! [this facts]
       (set! updates (conj updates (->PendingUpdate :retract facts))))

     (get-updates-and-reset! [this]
       (let [current-updates updates
             n (count current-updates)]
         (set! updates [])
         (if (zero? n)
           []
           (loop [i 0
                  result (transient [])]
             (if (< i n)
               (let [pu (nth current-updates i)
                     current-type (:type pu)
                     ;; Scan forward to find the end of this consecutive run
                     end (loop [j (inc i)]
                           (if (and (< j n)
                                    (= current-type (:type (nth current-updates j))))
                             (recur (inc j))
                             j))]
                 (if (= (inc i) end)
                   ;; Single update
                   (recur end (conj! result [current-type (:facts pu)]))
                   ;; Multiple — concat facts
                   (let [facts (loop [k i
                                      acc (transient [])]
                                 (if (< k end)
                                   (recur (inc k)
                                          (reduce conj! acc (:facts (nth current-updates k))))
                                   (persistent! acc)))]
                     (recur end (conj! result [current-type facts])))))
               (persistent! result))))))))

(defn get-ordered-update-cache
  []
  #?(:clj (OrderedUpdateCache. (ArrayList.))
     :cljs (OrderedUpdateCache. [])))

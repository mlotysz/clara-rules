(ns clara.tools.internal.inspect
  "Internal implementation details of session inspection.  Nothing in this namespace
   should be directly referenced by callers outside of the clara-rules project."
  (:require [clara.rules.listener :as l]
            [clara.rules.engine :as eng]))

(declare to-persistent-listener)

(deftype TransientActivationListener [activations]
  l/ITransientEventListener
  (fire-activation! [listener activation resulting-operations]
    (swap! (.-activations listener) conj {:activation activation
                                          :resulting-operations resulting-operations})
    listener)
  (to-persistent! [listener]
    (to-persistent-listener @(.-activations listener)))

  ;; The methods below don't do anything; they aren't needed for this functionality.
  (left-activate! [listener _node _tokens]
    listener)
  (left-retract! [listener _node _tokens]
    listener)
  (right-activate! [listener _node _elements]
    listener)
  (right-retract! [listener _node _elements]
    listener)
  (insert-facts! [listener _node _token _facts]
    listener)
  (alpha-activate! [listener _node _facts]
    listener)
  (insert-facts-logical! [listener _node _token _facts]
    listener)
  (retract-facts! [listener _node _token _facts]
    listener)
  (alpha-retract! [listener _node _facts]
    listener)
  (retract-facts-logical! [listener _node _token _facts]
    listener)
  (add-accum-reduced! [listener _node _join-bindings _result _fact-bindings]
    listener)
  (remove-accum-reduced! [listener _node _join-bindings _fact-bindings]
    listener)
  (add-activations! [listener _node _activations]
    listener)
  (remove-activations! [listener _node _activations]
    listener)
  (activation-group-transition! [listener _previous-group _new-group]
    listener)
  (fire-rules! [listener _node]
    listener))

(deftype PersistentActivationListener [activations]
  l/IPersistentEventListener
  (to-transient [_listener]
    (TransientActivationListener. (atom activations))))

(defn to-persistent-listener
  [activations]
  (PersistentActivationListener. activations))

(defn with-activation-listening
  [session]
  (if (empty? (eng/find-listeners session (partial instance? PersistentActivationListener)))
    (eng/with-listener session (PersistentActivationListener. []))
    session))

(defn without-activation-listening
  [session]
  (eng/remove-listeners session (partial instance? PersistentActivationListener)))

(defn get-activation-info
  [session]
  (let [matching-listeners (eng/find-listeners session (partial instance? PersistentActivationListener))]
    (condp = (count matching-listeners)
      0 nil
      1 (-> matching-listeners ^PersistentActivationListener (first) .-activations)
      (throw (ex-info "Found more than one PersistentActivationListener on session"
                      {:session session})))))

  

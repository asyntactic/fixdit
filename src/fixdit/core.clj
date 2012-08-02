(ns fixdit.core
  ;;(:require)
  (:use clojure.core
        [korma.core :only [insert values update set-fields where]]
        [clj-time.coerce :only [from-string to-timestamp]]
        [clj-yaml.core :only [parse-string]]))

(comment example input structure
         {"entity1" {"object1" {"field1" 1
                                "field2" "value"
                                "field3" 2.5}
                     "object2" {"field1" 5
                                "field2" "other value"
                                "field3" 6.7}}
          "entity2" [{"field" "value"
                      "entity1_id" "object1"}
                     {"field" "another value"
                      "entity1_id" "object2"}]})


(defn id-field-name? [name]
  (and (re-matches #".*_id"
                   (subs (str name) 1))
       name))

(defn convert-object-dates [field-map]
  (reduce-kv (fn [result-map field-name field-value]
               (let [name-string (subs (str field-name) 1)]
                 (if (or (re-matches #".*_date" name-string)
                         (re-matches #".*_timestamp" name-string)
                         (re-matches #".*_date_time" name-string)
                         (re-matches #".*_dt_tm" name-string))
                   (assoc result-map field-name
                          (to-timestamp (from-string field-value)))
                   result-map)))
             field-map field-map))


;; resolves entity names for a single instance referring to those names,
;; so that they can be inserted
;; takes entities structure and a map of {:blah_id "name" ...}
;; and returns a map of  {:blah_id id ...}
(defn resolved-entity-names [entities field-map]
  (reduce-kv (fn [result-map field-name instance-name]
               #_(println (keyword
                           (subs (str field-name)
                                 1 (- (.length
                                       (str field-name)) 3))))
               (assoc result-map field-name
                      (get-in entities [:instances
                                        (keyword instance-name)])))
             {} field-map))

;; this is provided as a quick way of switching between sqlite and postgresql
;; It is needed because they don't return the same data structure from
;; insert. :id doesn't work for sqlite, instead, it must be (val (first RESULT))
(defn id-func [x]
  ;; (:id x)
  (val (first x)))

(defn load-named-objects [entities entity-name objects]
  (let [entity (get-in entities [:entities entity-name])]
    (reduce-kv
     (fn [entities name instance]
       (let [resolved (resolved-entity-names
                       entities
                       (select-keys instance
                                    (filter id-field-name?
                                            (keys instance))))
             id (id-func (insert entity
                                 (values
                                  (convert-object-dates
                                   (merge instance resolved)))))]
         (if (some #(and (id-field-name? (key %)) (nil? (val %)))
                   resolved)
           ;; put resolved instance in :incomplete entry
           (assoc-in
            (assoc-in entities [:instances name] id)
            [:incomplete entity-name id]
            instance)
           (assoc-in entities [:instances name] id))))
     entities objects)))

(defn load-unnamed-objects [entities entity-name objects]
  (let [entity (get-in entities [:entities entity-name])]
    (reduce
     (fn [entities instance]
       (let [resolved (resolved-entity-names
                       entities
                       (select-keys instance
                                    (filter id-field-name?
                                            (keys instance))))
             id (id-func (insert entity
                                 (values
                                  (convert-object-dates
                                   (merge instance resolved)))))]
         (if (some #(and (id-field-name? (key %)) (nil? (val %)))
                   resolved)
           (assoc-in entities [:incomplete entity-name id] instance)
           entities)))
     entities objects)))

(defn update-object [entities entity id instance]
  (update entity
          (set-fields (filter #(id-field-name? (key %))
                              (resolved-entity-names entities instance)))
          (where {:id id})))

;; by the time this is called, all object names should be resolved
(defn update-incomplete-objects [entities]
  (doseq [inc-entry (:incomplete entities)]
    (let [entity (get-in entities [:entities (key inc-entry)])]
      (doseq [entry (val inc-entry)]
        (update-object entities entity (key entry) (val entry)))))
  entities)

(defn load-fixture-map [entity-namespace fixtures]
  (update-incomplete-objects
   (reduce-kv (fn [entities entity-name instances]
                (let [entity (var-get
                              (ns-resolve entity-namespace
                                          (symbol (subs
                                                   (str entity-name) 1))))
                      entities
                      (assoc-in entities [:entities entity-name] entity)]
                  (cond (seq? instances)
                        (load-unnamed-objects entities entity-name
                                              instances)
                        (map? instances)
                        (load-named-objects entities entity-name
                                            instances))))
              {}
              fixtures)))

(defn load-fixtures [{:keys [entity-namespace yaml-file yaml-string]}]
  (let [fixtures (or (and yaml-file (parse-string (slurp yaml-file)))
                     (parse-string yaml-string))]
    (load-fixture-map entity-namespace fixtures)))
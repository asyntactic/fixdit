(ns fixdit.core
  ;;(:require)
  (:use clojure.core
        [korma.core :only [insert values]]
        [clj-time.coerce :only [from-string to-sql-date]]
        [clj-yaml.core :only [parse-string]]))

(comment example structure
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
                          (to-sql-date (from-string field-value)))
                   result-map)))
             field-map field-map))


;; resolves entity names for a single instance referring to those names,
;; so that they can be inserted
;; takes entities structure and a map of {"blah_id" "name" ...}
;; and returns a map of  {"blah_id" id ...}
(defn resolved-entity-names [entities field-map]
  (reduce-kv (fn [result-map field-name instance-name]
               (assoc result-map field-name
                      (get-in entities [(keyword
                                         (subs (str field-name)
                                               1 (- (.length
                                                     (str field-name)) 3)))
                                        :instances
                                        (keyword instance-name)])))
             {} field-map))

(defn load-named-objects [entities entity-name objects]
  (let [entity (get-in entities [entity-name :entity])] 
    (reduce-kv
     (fn [entities name instance]
       (assoc-in
        entities [entity-name :instances name]
        ;; XXX :id doesn't work for sqlite
        ;; instead, it would be (val (first (insert ...)))
        (:id
         (insert entity
                 (values
                  (convert-object-dates
                   (merge instance
                          (resolved-entity-names
                           entities
                           (select-keys instance
                                        (filter id-field-name?
                                                (keys instance)))))))))))
     entities objects)))

(defn load-unnamed-objects [entities entity-name objects]
  (let [entity (get-in entities [entity-name :entity])]
    (doseq [instance objects]
      (insert entity
              (values
               (convert-object-dates
                (merge instance
                       (resolved-entity-names
                        entities
                        (select-keys instance
                                     (filter id-field-name?
                                             (keys instance))))))))))
  entities)

(defn load-fixtures [{:keys [entity-namespace yaml-file yaml-string]}]
  (let [fixtures (or (and yaml-file (parse-string (slurp yaml-file)))
                     (parse-string yaml-string))]
    (reduce-kv (fn [entities entity-name instances]
                 (let [entity (var-get
                               (ns-resolve entity-namespace
                                           (symbol (subs
                                                    (str entity-name) 1))))
                       entities
                       (assoc entities entity-name {:entity entity})]
                   (cond (seq? instances)
                         (load-unnamed-objects entities entity-name
                                               instances)
                         (map? instances)
                         (load-named-objects entities entity-name
                                             instances))))
               {}
               fixtures)))

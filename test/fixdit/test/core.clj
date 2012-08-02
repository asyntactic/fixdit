(ns fixdit.test.core
  ;;s(:require [fixdit.test.models :as models])
  (:refer-clojure :exclude [integer float drop insert values])
  (:use [clojure.test]
        fixdit.core
        (korma [core :only [defentity entity-fields has-many belongs-to
                            select where fields insert values sql-only]]
               [db :only [defdb sqlite3]])
        (lobos [core :only [create drop]]
               [schema :only [table integer float varchar]]
               [connectivity :only [open-global close-global]])
        [clj-time.core :only [date-time]]
        [clj-time.coerce :only [to-timestamp]]))

;; XXX there is currently a problem with fixit.core - in load-named-objects,
;; we look up the id of the inserted object. SQLite and PostgreSQL do not
;; have the same behavior for insert. The docs page of the Korma site says
;; it should return the first inserted id, while the API documentation says
;; it should return the last one. In fact, PostgreSQL returns a map of the
;; inserted object, while SQLite returns a map with the id as the value,
;; and some DB-specific string as the key.

(def fixdit-test-connection {:classname "org.sqlite.JDBC"
                             :subprotocol "sqlite"
                             :subname "./fixdit.test.sqlite3"
                             :create true})

(declare entity1 entity2 entity3 entity4 entity5)

(defentity entity1
  (entity-fields :field1 :field2 :field3)
  (has-many entity2))

(defentity entity2
  (entity-fields :field_a)
  (belongs-to entity1 (:fk :entity1_id)))

(defentity entity3
  (entity-fields :field1 :field2 :field3)
  (has-many entity2))

(defentity entity4
  (entity-fields :field_a)
  (belongs-to entity5 (:fk :entity5_id)))

(defentity entity5
  (entity-fields :field_b)
  (belongs-to entity4 (:fk :entity4_id)))

(defn fixture [f]
  (open-global fixdit-test-connection)
  (defdb test-db (sqlite3 fixdit-test-connection))
  (create (table :entity1
                 (integer :id :primary-key :auto-inc)
                 (integer :field1)
                 (varchar :field2 10)
                 (float :field3)))
  (create (table :entity2
                 (integer :id :primary-key :auto-inc)
                 (varchar :field_a 10)
                 (integer :entity1_id)))
  (create (table :entity3
                 (integer :field1)
                 (varchar :field2 10)
                 ;; id is not first - to test that it can be anywhere
                 (integer :id :primary-key :auto-inc)
                 (float :field3)))

  ;; these two tables mutually refer to each other, guaranteeing that there
  ;; will be an ordering problem where at least one row doesn't have one of
  ;; it's foreign-key ids at the time it's first inserted.
  (create (table :entity4
                 (integer :id :primary-key :auto-inc)
                 (integer :entity5_id)
                 (integer :field_a)))
  (create (table :entity5
                 (integer :id :primary-key :auto-inc)
                 (integer :entity4_id)
                 (integer :field_b)))
  (f)
  (drop (table :entity1))
  (drop (table :entity2))
  (drop (table :entity3))
  (drop (table :entity4))
  (drop (table :entity5))
  (close-global))

(use-fixtures :each fixture)

(deftest detect-id-field-names
  (is (not (nil? (id-field-name? :foobarbaz_id))))
  (is (not (nil? (id-field-name? :foo_bar_baz_id))))
  (is (nil? (id-field-name? :foobarbaz)))
  (is (nil? (id-field-name? :foo_bar_baz))))

(deftest convert-fields-to-dates
  (is (= (convert-object-dates {:id 1
                                :some_text "foobarbaz"
                                :test_date "2001-1-1"
                                :a_test_timestamp "2001-1-2"
                                :a_test_date_time "2001-1-3"
                                :a_test_dt_tm "2001-1-4"})
         {:id 1
          :some_text "foobarbaz"
          :test_date (to-timestamp (date-time 2001 1 1))
          :a_test_timestamp (to-timestamp (date-time 2001 1 2))
          :a_test_date_time (to-timestamp (date-time 2001 1 3))
          :a_test_dt_tm (to-timestamp (date-time 2001 1 4))})))

         
(deftest resolve-entity-names-single-field
  (is (= (resolved-entity-names {:entities {:entity1 entity1}
                                 :instances {:instance1 1
                                             :instance2 2}}
                                {:entity1_id "instance1"})
         {:entity1_id 1})))

(deftest resolve-entity-names-multiple-fields
  (is (= (resolved-entity-names {:entities {:entity1 entity1
                                            :entity entity2}
                                 :instances {:instance1 1
                                             :instance2 2
                                             :instance3 3
                                             :instance4 4}}
                                {:entity1_id "instance1"
                                 :entity2_id "instance4"})
         {:entity1_id 1
          :entity2_id 4})))

(deftest resolve-entity-names-id-field-not-entity
  (is (= (resolved-entity-names {:entities {:entity1 entity1}
                                 :instances {:instance1 1
                                             :instance2 2}}
                                {:something_else_entity1_id "instance1"})
         {:something_else_entity1_id 1})))

(deftest resolve-entity-names-missing-name
  (is (= (resolved-entity-names {:entities {:entity1 entity1}
                                 :instances {:instance1 1
                                             :instance2 2}}
                                {:entity1_id "instance3"})
         {:entity1_id nil})))

(deftest named-object-assigned-id
  (let [entities (load-named-objects {:entities {:entity1 entity1}
                                      :instances {}}
                                     :entity1
                                     {:object1 {:field1 1
                                                :field2 "value"
                                                :field3 2.5}})]
    (is (= (get-in entities [:instances :object1])
           (get-in (select entity1 (where {:field1 1}) (fields [:id]))
                   [0 :id])))))

(deftest named-object-assigned-id-not-beginning-with-id
  (let [entities (load-named-objects {:entities {:entity3 entity3}
                                      :instances {}}
                                     :entity3
                                     {:object1 {:field1 1
                                                :field2 "value"
                                                :field3 2.5}})]
    (is (= (get-in entities [:instances :object1])
           (get-in (select entity3 (where {:field1 1}) (fields [:id]))
                   [0 :id])))))

(deftest unnamed-objects-joined
  (let [entities (load-named-objects {:entities {:entity1 entity1
                                                 :entity2 entity2}
                                      :instances {}}
                                     :entity1
                                     {:object1 {:field1 1
                                                :field2 "value"
                                                :field3 2.5}})]
    (load-unnamed-objects entities :entity2
                          [{:field_a "value"
                            :entity1_id "object1"}])
    (is (= (get-in (select entity1 (where {:field1 1}) (fields [:id]))
                   [0 :id])
           (get-in (select entity2 (where {:field_a "value"})
                           (fields [:entity1_id]))
                   [0 :entity1_id])))))

(deftest named-objects-circular-dependency
  (let [fixtures {:entity4 {:object1 {:field_a 100
                                      :entity5_id "object2"}}
                  :entity5 {:object2 {:field_b 200
                                      :entity4_id "object1"}}}]
    (load-fixture-map 'fixdit.test.core fixtures)
    (let [data4 (first (select entity4 (where {:field_a 100})))
          data5 (first (select entity5 (where {:field_b 200})))]
      (clojure.pprint/pprint [data4 data5])
      (is (= (:id data4) (:entity4_id data5)))
      (is (= (:id data5) (:entity5_id data4))))))

(deftest load-fixtures-from-string
  (let [fixture-string
        (str "entity1:\n"
             "  object1:\n"
             "    field1: 1\n"
             "    field2: value\n"
             "    field3: 2.5\n"
             "  object2:\n"
             "     field1: 5\n"
             "     field2: other value\n"
             "     field3: 6.7\n"
             "entity2:\n"
             "  - field_a: value\n"
             "    entity1_id: object1\n"
             "  - field_a: another value\n"
             "    entity1_id: object2\n")]
    (load-fixtures {:entity-namespace 'fixdit.test.core
                    :yaml-string fixture-string}))
  (is (= (get-in (select entity1 (where {:field1 1}) (fields [:id]))
                 [0 :id])
         (get-in (select entity2 (where {:field_a "value"})
                         (fields [:entity1_id]))
                 [0 :entity1_id])))
  (is (= (get-in (select entity1 (where {:field1 5}) (fields [:id]))
                 [0 :id])
         (get-in (select entity2 (where {:field_a "another value"})
                         (fields [:entity1_id]))
                 [0 :entity1_id]))))

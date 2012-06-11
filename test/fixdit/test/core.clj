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
               [connectivity :only [open-global close-global]])))

(def fixdit-test-connection {:classname "org.sqlite.JDBC"
                             :subprotocol "sqlite"
                             :subname "./fixdit.test.sqlite3"
                             :create true})

(declare entity1 entity2)

(defentity entity1
  (entity-fields :field1 :field2 :field3)
  (has-many entity2))

(defentity entity2
  (entity-fields :field_a)
  (belongs-to entity1 (:fk :entity1_id)))

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
  (f)
  (drop (table :entity1))
  (drop (table :entity2))
  (close-global))

(use-fixtures :each fixture)

(deftest detect-id-field-names
  (is (not (nil? (id-field-name? :foobarbaz_id))))
  (is (not (nil? (id-field-name? :foo_bar_baz_id))))
  (is (nil? (id-field-name? :foobarbaz)))
  (is (nil? (id-field-name? :foo_bar_baz))))

(deftest resolve-entity-names-single-field
  (is (= (resolved-entity-names {:entity1 {:entity entity1
                                            :instances {:instance1 1
                                                        :instance2 2}}}
                                {:entity1_id "instance1"})
         {:entity1_id 1})))

(deftest resolve-entity-names-multiple-fields
  (is (= (resolved-entity-names {:entity1 {:entity entity1
                                            :instances {:instance1 1
                                                        :nstance2 2}}
                                 :entity2 {:entity entity2
                                            :instances {:instance3 3
                                                        :instance4 4}}}
                                {:entity1_id "instance1"
                                 :entity2_id "instance4"})
         {:entity1_id 1
          :entity2_id 4})))

(deftest resolve-entity-names-missing-name
  (is (= (resolved-entity-names {:entity1 {:entity entity1
                                            :instances {:instance1 1
                                                        :instance2 2}}}
                                {:entity1_id "instance3"})
         {:entity1_id nil})))

(deftest named-object-assigned-id
  (let [entities (load-named-objects {:entity1 {:entity entity1
                                                 :instances {}}}
                                     :entity1
                                     {:object1 {:field1 1
                                                :field2 "value"
                                                :field3 2.5}})]
    (is (= (get-in entities [:entity1 :instances :object1])
           (get-in (select entity1 (where {:field1 1}) (fields [:id]))
                   [0 :id])))))

(deftest unnamed-objects-joined
  (let [entities (load-named-objects {:entity1 {:entity entity1
                                                 :instances {}}
                                      :entity2 {:entity entity2
                                                 :instances {}}}
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

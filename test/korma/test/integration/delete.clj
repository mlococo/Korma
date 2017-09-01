(ns korma.test.integration.delete
  (:refer-clojure :exclude [update])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.trace :as trace])
  (:import (java.sql PreparedStatement))
  (:use clojure.test
        korma.core
        korma.db
        clojure.pprint
        clojure.reflect
        [korma.test.integration.helpers :only [populate address]]))

;; lots of these tests fail on purpose, pick one to run that is interesting to you
;; $ lein test :only korma.test.integration.delete/interop-no-race 

(defdb db (sqlite3 {:db "mike.db"}))
(use-fixtures :once
  (fn [f]
    (default-connection db)
    (f)))

(deftest interop-no-race
(let [korma-con (get-connection db)
      clojure-con (jdbc/get-connection korma-con)]
  (pprint (reflect clojure-con))
  (doseq [i (range 100)]
    (-> clojure-con
        (.prepareStatement "CREATE TABLE 'interop-race' ('id' TEXT PRIMARY KEY);")
        .executeUpdate)
    (-> clojure-con
        (.prepareStatement "DROP TABLE 'interop-race';")
        .executeUpdate))))

(deftest interop-close-statement-no-race
  (let [korma-con (get-connection db)
        clojure-con (jdbc/get-connection korma-con)]
    (doseq [i (range 1000)]
      (doseq [sql ["CREATE TABLE 'interop-close-statement-race' ('id' TEXT PRIMARY KEY);"
                   "DROP TABLE 'interop-close-statement-race';"]]
        (with-open [^PreparedStatement prep (.prepareStatement clojure-con sql)]
          (.executeUpdate prep))))))

(deftest interop-return-keys-no-race
  (let [korma-con (get-connection db)
        clojure-con (jdbc/get-connection korma-con)]
    (doseq [i (range 1000)]
      (doseq [sql ["CREATE TABLE 'interop-return-keys-race' ('id' TEXT PRIMARY KEY);"
                   "DROP TABLE 'interop-return-keys-race';"]]
        (with-open [^PreparedStatement prep (.prepareStatement clojure-con sql)]
          (.executeUpdate prep)
          (-> prep
              .getGeneratedKeys
              .close))))))

(deftest jdbc-race-no-transaction
  (trace/trace jdbc/db-do-prepared)
  (let [conn (get-connection db)]
    (doseq [i (range 100)]
      (jdbc/db-do-prepared conn false "CREATE TABLE 'jdbc-race-no-transaction' ('id' TEXT PRIMARY KEY);")
      (jdbc/db-do-prepared conn false "DROP TABLE 'jdbc-race-no-transaction';"))))
;; lein test :only korma.test.integration.delete/jdbc-race-no-transaction

;; ERROR in (jdbc-race-no-transaction) (DB.java:909)
;; Uncaught exception, not in assertion.
;; expected: nil
;;   actual: org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (table 'jdbc-race-no-transaction' already exists)
;;  at org.sqlite.core.DB.newSQLException (DB.java:909)
;;     org.sqlite.core.DB.newSQLException (DB.java:921)
;;     org.sqlite.core.DB.throwex (DB.java:886)
;;     org.sqlite.core.NativeDB.prepare_utf8 (NativeDB.java:-2)
;;     org.sqlite.core.NativeDB.prepare (NativeDB.java:127)
;;     org.sqlite.core.DB.prepare (DB.java:227)
;;     org.sqlite.core.CorePreparedStatement.<init> (CorePreparedStatement.java:41)
;;     org.sqlite.jdbc3.JDBC3PreparedStatement.<init> (JDBC3PreparedStatement.java:30)
;;     org.sqlite.jdbc4.JDBC4PreparedStatement.<init> (JDBC4PreparedStatement.java:19)
;;     org.sqlite.jdbc4.JDBC4Connection.prepareStatement (JDBC4Connection.java:48)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:263)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:235)
;;     com.mchange.v2.c3p0.impl.NewProxyConnection.prepareStatement (NewProxyConnection.java:567)
;;     clojure.java.jdbc$prepare_statement.invokeStatic (jdbc.clj:495)
;;     clojure.java.jdbc$prepare_statement.invoke (jdbc.clj:454)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:813)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:816)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:806)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     korma.test.integration.delete$fn__1562.invokeStatic (delete.clj:30)
;;     korma.test.integration.delete/fn (delete.clj:27)

(deftest jdbc-race
  (let [conn (get-connection db)]
    (doseq [i (range 100)]
      (jdbc/db-do-prepared conn "CREATE TABLE 'jdbc-race' ('id' TEXT PRIMARY KEY);")
      (jdbc/db-do-prepared conn "DROP TABLE 'jdbc-race';"))))
;; lein test :only korma.test.integration.delete/jdbc-race

;; ERROR in (jdbc-race) (DB.java:909)
;; Uncaught exception, not in assertion.
;; expected: nil
;;   actual: org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (table 'jdbc-race' already exists)
;;  at org.sqlite.core.DB.newSQLException (DB.java:909)
;;     org.sqlite.core.DB.newSQLException (DB.java:921)
;;     org.sqlite.core.DB.throwex (DB.java:886)
;;     org.sqlite.core.NativeDB.prepare_utf8 (NativeDB.java:-2)
;;     org.sqlite.core.NativeDB.prepare (NativeDB.java:127)
;;     org.sqlite.core.DB.prepare (DB.java:227)
;;     org.sqlite.core.CorePreparedStatement.<init> (CorePreparedStatement.java:41)
;;     org.sqlite.jdbc3.JDBC3PreparedStatement.<init> (JDBC3PreparedStatement.java:30)
;;     org.sqlite.jdbc4.JDBC4PreparedStatement.<init> (JDBC4PreparedStatement.java:19)
;;     org.sqlite.jdbc4.JDBC4Connection.prepareStatement (JDBC4Connection.java:48)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:263)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:235)
;;     com.mchange.v2.c3p0.impl.NewProxyConnection.prepareStatement (NewProxyConnection.java:567)
;;     clojure.java.jdbc$prepare_statement.invokeStatic (jdbc.clj:495)
;;     clojure.java.jdbc$prepare_statement.invoke (jdbc.clj:454)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:813)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:816)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:802)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     korma.test.integration.delete$fn__1553.invokeStatic (delete.clj:19)
;;     korma.test.integration.delete/fn (delete.clj:16)
;;     ... truncated once we hit the test harness...

(deftest drop-table-race
  (doseq [i (range 100)]
    (exec-raw "CREATE TABLE 'droprace' ('id' TEXT PRIMARY KEY);")
    (exec-raw "DROP TABLE 'droprace';")))
;; lein test :only korma.test.integration.delete/drop-table-race

;; ERROR in (drop-table-race) (DB.java:909)
;; Uncaught exception, not in assertion.
;; expected: nil
;;   actual: org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (table 'droprace' already exists)
;;  at org.sqlite.core.DB.newSQLException (DB.java:909)
;;     org.sqlite.core.DB.newSQLException (DB.java:921)
;;     org.sqlite.core.DB.throwex (DB.java:886)
;;     org.sqlite.core.NativeDB.prepare_utf8 (NativeDB.java:-2)
;;     org.sqlite.core.NativeDB.prepare (NativeDB.java:127)
;;     org.sqlite.core.DB.prepare (DB.java:227)
;;     org.sqlite.core.CorePreparedStatement.<init> (CorePreparedStatement.java:41)
;;     org.sqlite.jdbc3.JDBC3PreparedStatement.<init> (JDBC3PreparedStatement.java:30)
;;     org.sqlite.jdbc4.JDBC4PreparedStatement.<init> (JDBC4PreparedStatement.java:19)
;;     org.sqlite.jdbc4.JDBC4Connection.prepareStatement (JDBC4Connection.java:48)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:263)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:235)
;;     com.mchange.v2.c3p0.impl.NewProxyConnection.prepareStatement (NewProxyConnection.java:567)
;;     clojure.java.jdbc$prepare_statement.invokeStatic (jdbc.clj:495)
;;     clojure.java.jdbc$prepare_statement.invoke (jdbc.clj:454)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:813)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:802)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     korma.db$exec_sql.invokeStatic (db.clj:296)
;;     korma.db$exec_sql.invoke (db.clj:290)
;;     korma.db$do_query.invokeStatic (db.clj:310)
;;     korma.db$do_query.invoke (db.clj:306)
;;     korma.core$exec_raw.invokeStatic (core.clj:531)
;;     korma.core$exec_raw.doInvoke (core.clj:518)
;;     clojure.lang.RestFn.invoke (RestFn.java:410)
;;     korma.test.integration.delete$fn__1562.invokeStatic (delete.clj:24)
;;     korma.test.integration.delete/fn (delete.clj:22)
;;     ... omitted once we hit the test harness functions...

(deftest drop-table-if-exists-race
  (doseq [i (range 100)]
    (exec-raw "DROP TABLE IF EXISTS 'dropifexistsrace';")
    (exec-raw "CREATE TABLE 'dropifexistsrace' ('id' TEXT PRIMARY KEY);")))
;; lein test :only korma.test.integration.delete/drop-table-if-exists-race

;; ERROR in (drop-table-if-exists-race) (DB.java:909)
;; Uncaught exception, not in assertion.
;; expected: nil
;;   actual: org.sqlite.SQLiteException: [SQLITE_ERROR] SQL error or missing database (table 'dropifexistsrace' already exists)
;;  at org.sqlite.core.DB.newSQLException (DB.java:909)
;;     org.sqlite.core.DB.newSQLException (DB.java:921)
;;     org.sqlite.core.DB.throwex (DB.java:886)
;;     org.sqlite.core.NativeDB.prepare_utf8 (NativeDB.java:-2)
;;     org.sqlite.core.NativeDB.prepare (NativeDB.java:127)
;;     org.sqlite.core.DB.prepare (DB.java:227)
;;     org.sqlite.core.CorePreparedStatement.<init> (CorePreparedStatement.java:41)
;;     org.sqlite.jdbc3.JDBC3PreparedStatement.<init> (JDBC3PreparedStatement.java:30)
;;     org.sqlite.jdbc4.JDBC4PreparedStatement.<init> (JDBC4PreparedStatement.java:19)
;;     org.sqlite.jdbc4.JDBC4Connection.prepareStatement (JDBC4Connection.java:48)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:263)
;;     org.sqlite.jdbc3.JDBC3Connection.prepareStatement (JDBC3Connection.java:235)
;;     com.mchange.v2.c3p0.impl.NewProxyConnection.prepareStatement (NewProxyConnection.java:567)
;;     clojure.java.jdbc$prepare_statement.invokeStatic (jdbc.clj:495)
;;     clojure.java.jdbc$prepare_statement.invoke (jdbc.clj:454)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:813)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     clojure.java.jdbc$db_do_prepared.invokeStatic (jdbc.clj:802)
;;     clojure.java.jdbc$db_do_prepared.invoke (jdbc.clj:795)
;;     korma.db$exec_sql.invokeStatic (db.clj:296)
;;     korma.db$exec_sql.invoke (db.clj:290)
;;     korma.db$do_query.invokeStatic (db.clj:310)
;;     korma.db$do_query.invoke (db.clj:306)
;;     korma.core$exec_raw.invokeStatic (core.clj:531)
;;     korma.core$exec_raw.doInvoke (core.clj:518)
;;     clojure.lang.RestFn.invoke (RestFn.java:410)
;;     korma.test.integration.delete$fn__1571.invokeStatic (delete.clj:30)
;;     korma.test.integration.delete/fn (delete.clj:27)
;;     ... truncated once we hit the test harness...

(deftest delete-with-fk
  (defentity fruit)
  (defentity person
    (has-one fruit))
  (doseq [statement
          ["DROP TABLE IF EXISTS 'person';"
           "DROP TABLE IF EXISTS 'fruit';"
           "CREATE TABLE 'fruit' ('id' TEXT PRIMARY KEY)"
           "CREATE TABLE 'person' ('fruit_id' TEXT, FOREIGN KEY(fruit_id) REFERENCES fruit(id));"
           "PRAGMA foreign_keys = ON;"]]
    (exec-raw statement))
  (doseq [i (range 10)]
    (is (= '(1) (vals (insert fruit (values {:id i})))))
    (is (= '(1) (vals (insert person (values {:fruit_id i})))))
    (is (= 1 (delete person (where {:fruit_id [= i]}))))
    (is (= 1 (delete fruit (where {:id [= i]}))))
    (is (empty? (select fruit)))
    (is (empty? (select person)))))

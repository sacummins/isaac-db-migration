(ns migration.core
  (:gen-class)
  (:use [clojure.pprint])
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [crypto.random :as rand]
            [clojure.data.json :as json])
  (:import (java.util Date)
           (java.sql Timestamp)
           (org.postgresql.util PSQLException PGobject)))

(def user-field-mappings {:_id       (fn [old-id] (str old-id))
                     :school_id #(Integer/parseInt %)
                     :last_seen #(if (instance? Long %)
                                  (Date. %)
                                  %)})

(def linked-account-field-mappings {:_id       (fn [old-id] (str old-id))
                                    })

(def logged-event-field-mappings {:event_details (fn [map] (doto (PGobject.)
                                                             (.setType "jsonb")
                                                             (.setValue (json/write-str map :value-fn (fn [k v]
                                                                                                        (if (instance? Date v)
                                                                                                          (.getTime v)
                                                                                                          v))))))
                                  :ip_address    (fn [v] (doto (PGobject.)
                                                           (.setType "inet")
                                                           (.setValue (let [s (first (str/split (or v "") #","))]
                                                                        (if (or (= s "") (= s "unknown"))
                                                                          nil
                                                                          s)))))
                                  })


(def field-name-mapping {:localUserId :user_id})

(def pg-db {:classname   "org.postgresql.Driver"
            :subprotocol "postgresql"
            :subname     "//localhost/rutherford"})

(defmacro with-mongo-conn [[c e] & body]
  `(let [~c ~e]
    ~@body
    (mg/disconnect ~c)
    (println "Disconnected")))

(defn migrate-keys [m]
  (apply merge (map (fn [[k v]]
                      (let [ nk (-> (or (get field-name-mapping k) k)
                                    (name)
                                    (str/replace #"[A-Z]" "_$0")
                                    (.toLowerCase)
                                    (keyword))]
                        {nk v})) m)))

(defn migrate-dates [m]
  (apply merge (map (fn [[k v]]
                      (cond
                        (instance? Date v) {k (Timestamp. (.getTime v))}
                        (map? v) {k (migrate-dates v)}
                        :else {k v})) m)))

(defn ensure-email [m]
  (assoc m :email (or (:email m) (str (rand/hex 6) "@null"))))

(defn migrate-fields [field-mappings m]
  (apply merge (map (fn [[k v]]
                      (if-let [f (k field-mappings)]
                        {k (f v)}
                        {k v})) m)))


(def args-with-values ["pg-user", "pg-passwd"])

(defn -main [& args]
  (let [args (let [next-val (atom nil)]
               (reduce (fn [a x]
                         (if-let [nv @next-val]
                           (do
                             (reset! next-val nil)
                             (assoc a (keyword nv) x))
                           (do
                             (when (some #(= % x) args-with-values)
                               (reset! next-val x))
                             (assoc a (keyword x) nil))))
                       {} args)
               )]


    (def pg-db (assoc pg-db :user (:pg-user args)
                            :password (:pg-passwd args)))

    (with-mongo-conn [conn (mg/connect)]
      (let [db (mg/get-db conn "rutherford")
            users (mc/find-maps db "users")
            linked-accounts (mc/find-maps db "linkedAccounts")
            logged-events (mc/find-maps db "loggedEvents")]

        (println "Loaded" (count users) "users")
        (println "Loaded" (count linked-accounts) "linkedAccounts")
        ;(println "Loaded" (count logged-events) "loggedEvents")

        (let [migrated-users (map (comp migrate-dates
                                        ensure-email
                                        (partial migrate-fields user-field-mappings)
                                        migrate-keys) users)]

          (if (contains? args :truncate)
            (do
              (println "TRUNCATING users, logged_events and linked_accounts")
              (jdbc/execute! pg-db ["TRUNCATE users CASCADE"])
              (jdbc/execute! pg-db ["TRUNCATE logged_events"])))

          (println "Inserting all user data into postgres")
          (apply jdbc/insert! pg-db :users migrated-users)

          #_(doseq [u migrated-users]
              (try
                (jdbc/insert! pg-db :users u)
                (catch PSQLException e
                  (println "Failed to add User" u (.getMessage e))
                  (flush))))

          (let [user-legacy-id-map (apply merge (map (fn [{mongo-id :_id pg-id :id}]
                                                       {mongo-id pg-id})
                                                     (jdbc/query pg-db ["SELECT _id, id FROM users"])))

                migrated-linked-accounts (map (comp #(dissoc % :_id)
                                                    #(assoc % :user_id (get user-legacy-id-map (:user_id %)))
                                                    (partial migrate-fields linked-account-field-mappings)
                                                    migrate-keys) linked-accounts)

                migrated-logged-events (map (comp   migrate-dates
                                                    #(dissoc % :_id)
                                                    #(assoc % :user_id (get user-legacy-id-map (:user_id %)))
                                                    (partial migrate-fields logged-event-field-mappings)
                                                    migrate-keys) logged-events)]

            (println "Inserting all linked account data into postgres")
            (apply jdbc/insert! pg-db :linked_accounts migrated-linked-accounts)

            (println "Inserting all log events data into postgres")
            #_(apply jdbc/insert! pg-db :logged_events migrated-logged-events)

            (doseq [es (partition-all 100000 migrated-logged-events)]
              (apply jdbc/insert! pg-db :logged_events es)
              (println "chunk inserted")
              (flush))

            (println "Migrated: ")
            (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM users"]))) "total users")
            (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM linked_accounts"]))) "total linked accounts")
            (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM logged_events"]))) "total logged events")
            )))))

  (println "Migration Completed"))
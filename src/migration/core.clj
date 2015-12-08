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

(def gameboard-field-mappings {:_id       (fn [old-id] (str old-id))
                               :wildcard (fn [map] (doto (PGobject.)
                                                             (.setType "jsonb")
                                                             (.setValue (json/write-str map :value-fn (fn [k v]
                                                                                                        (if (instance? Date v)
                                                                                                          (.getTime v)
                                                                                                          v))))))
                               :game_filter (fn [map] (doto (PGobject.)
                                                          (.setType "jsonb")
                                                          (.setValue (json/write-str map :value-fn (fn [k v]
                                                                                                     (if (instance? Date v)
                                                                                                       (.getTime v)
                                                                                                       v))))))
                                  })

; this is voodoo as far as I am concerned
(extend-protocol clojure.java.jdbc/ISQLParameter
  clojure.lang.IPersistentVector
  (set-parameter [v ^java.sql.PreparedStatement stmt ^long i]
    (let [conn (.getConnection stmt)
          meta (.getParameterMetaData stmt)
          type-name (.getParameterTypeName meta i)]
      (if-let [elem-type (when (= (first type-name) \_) (apply str (rest type-name)))]
        (.setObject stmt i (.createArrayOf conn elem-type (to-array v)))
        (.setObject stmt i v)))))

(extend-protocol clojure.java.jdbc/IResultSetReadColumn
  java.sql.Array
  (result-set-read-column [val _ _]
    (into [] (.getArray val))))
; end of voodoo


(def field-name-mapping {:wildCard :wildcard
                         :wildCardPosition :wildcard_position
                         :_id :id})

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
            users-to-gameboards (mc/find-maps db "UsersToGameboards")
            gameboards (mc/find-maps db "gameboards")]

        (if (contains? args :truncate)
          (do
            (println "TRUNCATING gameboards and users-to-gameboards")
            (jdbc/execute! pg-db ["TRUNCATE gameboards RESTART IDENTITY CASCADE "])
            (jdbc/execute! pg-db ["TRUNCATE user_gameboards RESTART IDENTITY CASCADE"])))


        (let [user-legacy-id-map (apply merge (map (fn [{mongo-id :_id pg-id :id}]
                                                     {mongo-id pg-id})
                                                   (jdbc/query pg-db ["SELECT _id, id FROM users"])))

              migrated-gameboards (map (comp migrate-dates
                                             #(assoc % :owner_user_id (if (get user-legacy-id-map (:owner_user_id %))
                                                                  (get user-legacy-id-map (:owner_user_id %))
                                                                  nil))
                                             (partial migrate-fields gameboard-field-mappings)
                                             migrate-keys) gameboards)

              migrated-user-gameboards (map (comp migrate-dates
                                                  #(dissoc % :id)
                                                  #(assoc % :user_id (get user-legacy-id-map (:user_id %)))
                                                  (partial migrate-fields gameboard-field-mappings)
                                                  migrate-keys) users-to-gameboards)]

          (println "Inserting all gameboards data into postgres")
          (pprint (take 1 migrated-gameboards))

          (doseq [es (partition-all 1000 migrated-gameboards)]
              (apply jdbc/insert! pg-db :gameboards es)
              (println "chunk inserted")
              (flush))

          (println "Inserting all user_gameboards data into postgres")
          (pprint (take 1 migrated-user-gameboards))
          (doseq [mugb migrated-user-gameboards]
              (try
                (jdbc/insert! pg-db :user_gameboards mugb)
                (catch PSQLException e
                  (println "Failed to add user link to Gameboard" mugb (.getMessage e))
                  (flush))))

          (println "Migrated: ")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM gameboards"]))) "total gameboards")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM user_gameboards"]))) "total user_gameboards")
          ))))

  (println "Migration Completed"))
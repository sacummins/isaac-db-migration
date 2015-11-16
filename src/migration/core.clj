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

(def question-attempt-field-mappings {:question_attempt (fn [map] (doto (PGobject.)
                                                             (.setType "jsonb")
                                                             (.setValue (json/write-str map :value-fn (fn [k v]
                                                                                                        (if (instance? Date v)
                                                                                                          (.getTime v)
                                                                                                          v))))))
                                  })

(defn mapp->pgobject [m] (doto (PGobject.)
                          (.setType "jsonb")
                          (.setValue (json/write-str m :value-fn (fn [_ v]
                                                                     (if (instance? Date v)
                                                                       (.getTime v)
                                                                       v))))))


(def field-name-mapping {:localUserId :user_id
                         :question_attempts :question_attempt})

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
            ;users (mc/find-maps db "users")
            ;linked-accounts (mc/find-maps db "linkedAccounts")
            question-attempts (mc/find-maps db "questionAttempts")]

        #_(doseq [u migrated-users]
            (try
              (jdbc/insert! pg-db :users u)
              (catch PSQLException e
                (println "Failed to add User" u (.getMessage e))
                (flush))))

        (let [user-legacy-id-map (apply merge (map (fn [{mongo-id :_id pg-id :id}]
                                                     {mongo-id pg-id})
                                                   (jdbc/query pg-db ["SELECT _id, id FROM users"])))


              #_migrated-question-attempts #_(map (comp migrate-dates
                                                #(dissoc % :_id)
                                                #(assoc % :user_id (if (get user-legacy-id-map (:user_id %))
                                                                     (get user-legacy-id-map (:user_id %))
                                                                     (:user_id %)))
                                                (partial migrate-fields question-attempt-field-mappings)
                                                migrate-keys) question-attempts)

              migrated-question-attempts (apply concat (map (fn [x]
                                                              (let [user-id (:userId x)
                                                                    question-page-attempts (:questionAttempts x)]
                                                                (let [question-part-attempts (map second question-page-attempts)
                                                                      question-part-attempts (apply merge question-part-attempts)]

                                                                  (apply concat (map (fn [[question-id attempts]]
                                                                                       (filter identity (map (fn [attempt]
                                                                                                               (if-let [new-user-id (get user-legacy-id-map user-id)]
                                                                                                                 {:user_id          new-user-id
                                                                                                                  :question_id      (name question-id)
                                                                                                                  :question_attempt (mapp->pgobject attempt)
                                                                                                                  :correct          (:correct attempt)
                                                                                                                  :timestamp        (Timestamp. (if (instance? Date (:dateAttempted attempt))
                                                                                                                                                  (.getTime (:dateAttempted attempt))
                                                                                                                                                  (:dateAttempted attempt)))
                                                                                                                  }
                                                                                                                 nil)) attempts))) question-part-attempts))))
                                                              ) question-attempts) )]


          (pprint (take 1 migrated-question-attempts))

          (println "Inserting all question data into postgres")
          ;(apply jdbc/insert! pg-db :users question-attempts)
          (if (contains? args :truncate)
            (do
              (println "TRUNCATING question_attempts")
              (jdbc/execute! pg-db ["TRUNCATE question_attempts CASCADE"])))


          (doseq [es (partition-all 10000 migrated-question-attempts)]
            (apply jdbc/insert! pg-db :question_attempts es)
            (println "question attempt chunk inserted")
            (flush))

          (println "Migrated: ")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM question_attempts"]))) "total questionAttempts")
          ))))

  (println "Migration Completed"))
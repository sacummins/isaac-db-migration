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
            groups (mc/find-maps db "userGroups")
            group-memberships (mc/find-maps db "groupMemberships")
            assignments (mc/find-maps db "assignments")
            user-associations (mc/find-maps db "userAssociations")
            user-association-tokens (mc/find-maps db "userAssociationsTokens")]

        (let [user-legacy-id-map (apply merge (map (fn [{mongo-id :_id pg-id :id}]
                                                     {mongo-id pg-id})
                                                   (jdbc/query pg-db ["SELECT _id, id FROM users"])))

              migrated-groups (map (comp migrate-dates
                                         #(assoc % :owner_id (if-let [new-id (get user-legacy-id-map (:owner_id %))]
                                                                new-id
                                                                (:owner_id %)))
                                         migrate-keys) groups)

              #_migrated-question-attempts #_(map (comp migrate-dates
                                                #(dissoc % :_id)
                                                #(assoc % :user_id (if (get user-legacy-id-map (:user_id %))
                                                                     (get user-legacy-id-map (:user_id %))
                                                                     (:user_id %)))
                                                (partial migrate-fields question-attempt-field-mappings)
                                                migrate-keys) question-attempts)]


          (pprint (take 1 migrated-groups))

          (println "Inserting all group data into postgres")
          ;(apply jdbc/insert! pg-db :users question-attempts)
          (if (contains? args :truncate)
            (do
              (println "TRUNCATING groups, group_memberships, assignments, user_associations and user_associations_tokens")
              (jdbc/execute! pg-db ["TRUNCATE groups RESTART IDENTITY CASCADE"])
              (jdbc/execute! pg-db ["TRUNCATE group_memberships RESTART IDENTITY CASCADE"])
              (jdbc/execute! pg-db ["TRUNCATE assignments RESTART IDENTITY CASCADE"])
              (jdbc/execute! pg-db ["TRUNCATE user_associations RESTART IDENTITY CASCADE"])
              (jdbc/execute! pg-db ["TRUNCATE user_associations_tokens RESTART IDENTITY CASCADE"])))

          (let [group-legacy-id-map (apply merge (for [g migrated-groups]
                                                   (try (let [new-group-id (:id (first (jdbc/insert! pg-db :groups (dissoc g :_id))))]
                                                          {(str (:_id g)) new-group-id})
                                                        (catch Exception e
                                                          (println "failed to add group" g (.getMessage e))))

                                                   ))

                migrated-group-memberships (map (comp migrate-dates
                                                      #(dissoc % :_id)

                                                      #(assoc % :user_id (if-let [new-id (get user-legacy-id-map (:user_id %))]
                                                                           new-id
                                                                           (:user_id %))
                                                                :group_id (if-let [new-id (get group-legacy-id-map (:group_id %))]
                                                                            new-id
                                                                            (:group_id %)))
                                                      migrate-keys) group-memberships)
                migrated-assignments (map (comp migrate-dates
                                                #(dissoc % :_id)

                                                #(assoc % :owner_user_id (if-let [new-id (get user-legacy-id-map (:owner_user_id %))]
                                                                     new-id
                                                                     (:owner_user_id %))
                                                          :group_id (if-let [new-id (get group-legacy-id-map (:group_id %))]
                                                                      new-id
                                                                      (:group_id %)))
                                                migrate-keys) assignments)

                migrated-associations (map (comp migrate-dates
                                                 #(dissoc % :_id)

                                                 #(assoc % :user_id_granting_permission (if-let [new-id (get user-legacy-id-map (:user_id_granting_permission %))]
                                                                            new-id
                                                                            (:user_id_granting_permission %))
                                                           :user_id_receiving_permission (if-let [new-id (get user-legacy-id-map (:user_id_receiving_permission %))]
                                                                                          new-id
                                                                                          (:user_id_receiving_permission %)))
                                                 migrate-keys) user-associations)

                migrated-association-tokens (map (comp migrate-dates
                                                       #(dissoc % :_id)

                                                       #(assoc % :owner_user_id (if-let [new-id (get user-legacy-id-map (:owner_user_id %))]
                                                                                                new-id
                                                                                                (:owner_user_id %))
                                                                 :group_id (if-let [new-id (get group-legacy-id-map (:group_id %))]
                                                                             new-id
                                                                             (:group_id %)))
                                                       migrate-keys) user-association-tokens)]

            (println "Inserting group membership information")

            (doseq [m migrated-group-memberships]
              (try
                (jdbc/insert! pg-db :group_memberships m)
                (catch Exception e
                  (println "Could not insert group membership" m (.getMessage e)))))

            (println "Inserting assignment information")

            (doseq [a migrated-assignments]
              (try
                (jdbc/insert! pg-db :assignments a)
                (catch Exception e
                  (println "Could not insert assignment" a (.getMessage e)))))


            (println "Inserting user association information")

            (doseq [a migrated-associations]
              (try
                (jdbc/insert! pg-db :user_associations a)
                (catch Exception e
                  (println "Could not insert association" a (.getMessage e)))))

            (println "Inserting user association tokens information")

            (doseq [a migrated-association-tokens]
              (try
                (jdbc/insert! pg-db :user_associations_tokens a)
                (catch Exception e
                  (println "Could not insert association token" a (.getMessage e)))))


            #_(doseq [es (partition-all 10000 migrated-group-memberships)]
              (apply jdbc/insert! pg-db :group_memberships es)
              (println "group membership chunk inserted")
              (flush)))

          (println "Migrated: ")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM groups"]))) "total groups")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM group_memberships"]))) "total groups_memberships")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM assignments"]))) "total assignments")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM user_associations"]))) "total user_associations")
          (println (:count (first (jdbc/query pg-db ["SELECT count(*) FROM user_associations_tokens"]))) "total user_associations_tokens")
          ;(println @invalid-users "Could not be resolved so there question attempts were not imported.")
          ))))

  (println "Migration Completed"))
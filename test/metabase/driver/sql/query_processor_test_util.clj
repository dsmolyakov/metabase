(ns metabase.driver.sql.query-processor-test-util
  (:require [clojure.string :as str]))

(defn pretty-sql [s]
  (if-not (string? s)
    s
    (-> s
        (str/replace #"\"([\w\d_-]+)\"" "$1")
        (str/replace #"PUBLIC\." ""))))

(defn even-prettier-sql [s]
  (-> s
      pretty-sql
      (str/replace #"\s+" " ")
      (str/replace #"\(\s*" "(")
      (str/replace #"\s*\)" ")")
      (str/replace #"PUBLIC\." "")
      (str/replace #"'" "\"")
      str/trim))

(defn- symbols [s]
  (binding [*read-eval* false]
    (read-string (str \( s \)))))

(def ^:private sql-keywords
  '#{[LEFT JOIN]
     [GROUP BY]
     [ORDER BY]
     SELECT
     FROM
     LIMIT
     WHERE
     OFFSET})

(defn- sql-map
  "Convert a sequence of SQL symbols into something sorta like a HoneySQL map. The main purpose of this is to make tests
  somewhat possible to debug. The goal isn't to actually be HoneySQL, but rather to make diffing huge maps easy."
  [symbols]
  (if-not (sequential? symbols)
    symbols
    (loop [m {}, current-key nil, [x & [y :as more]] symbols]
      (cond
        ;; two-word "keywords"
        (sql-keywords [x y])
        (let [x-y (keyword (str/lower-case (format "%s-%s" (name x) (name y))))]
          (recur m x-y (rest more)))

        ;; one-word keywords
        (sql-keywords x)
        (let [x (keyword (str/lower-case x))]
          (recur m x more))

        ;; if we stumble upon a nested sequence that starts with SQL keyword(s) then recursively transform that into a
        ;; map (e.g. for things like subselects)
        (and (sequential? x)
             (or (sql-keywords (take 2 x))
                 (sql-keywords (first x))))
        (recur m current-key (cons (sql-map x) more))

        :else
        (let [m (update m current-key #(conj (vec %) x))]
          (if more
            (recur m current-key more)
            m))))))

(defn sql->sql-map [sql]
  (-> sql even-prettier-sql symbols sql-map))

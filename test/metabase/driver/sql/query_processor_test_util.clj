(ns metabase.driver.sql.query-processor-test-util
  (:require [clojure.string :as str]))

(defn pretty-sql [s]
  (if-not (string? s)
    s
    (-> s
        (str/replace #"\"([\w\d_]+)\"" "$1")
        (str/replace #"PUBLIC\." ""))))

(defn even-prettier-sql [s]
  (-> s
      pretty-sql
      (str/replace #"\s+" " ")
      (str/replace #"\(\s*" "(")
      (str/replace #"\s*\)" ")")
      (str/replace #"PUBLIC\." "")
      str/trim))

(defn- symbols [s]
  (binding [*read-eval* false]
    (read-string (str \( s \)))))

(defn- sql-map
  "Convert a sequence of SQL symbols into something sorta like a HoneySQL map. The main purpose of this is to make tests
  somewhat possible to debug. The goal isn't to actually be HoneySQL, but rather to make diffing huge maps easy."
  [symbols]
  (if-not (sequential? symbols)
    symbols
    (loop [m {}, current-key nil, [x & [y :as more]] symbols]
      (cond
        ;; two-word "keywords"
        ('#{[LEFT JOIN] [GROUP BY] [ORDER BY]} [x y])
        (let [x-y (keyword (str/lower-case (format "%s-%s" (name x) (name y))))]
          (recur m x-y (rest more)))

        ;; one-word keywords
        ('#{SELECT FROM LIMIT WHERE OFFSET} x)
        (let [x (keyword (str/lower-case x))]
          (recur m x more))

        (and (sequential? x)
             (some? current-key)
             (empty? (get m current-key)))
        (recur m current-key (cons (sql-map x) more))

        :else
        (let [m (update m current-key #(conj (vec %) x))]
          (if more
            (recur m current-key more)
            m))))))

(defn sql->sql-map [sql]
  (-> sql even-prettier-sql symbols sql-map))

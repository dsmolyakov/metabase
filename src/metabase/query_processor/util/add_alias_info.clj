(ns metabase.query-processor.util.add-alias-info
  (:require [metabase.mbql.util :as mbql.u]
            [clojure.walk :as walk]
            [metabase.util.i18n :refer [tru]]
            [metabase.query-processor.store :as qp.store]
            [metabase.query-processor.error-type :as qp.error-type]))

;; TODO -- we should probably do clause normalization here so this doesn't break if we have extra info like the stuff we
;; add here

(defn- selected-clauses [{:keys [fields breakout aggregation]}]
  (into
   {}
   (comp cat
         (map-indexed
          (fn [i clause]
            [clause i])))
   [breakout
    (map-indexed
     (fn [i ag]
       (mbql.u/replace ag
         [:aggregation-options wrapped opts]
         [:aggregation i]

         ;; aggregation clause should be preprocessed into an `:aggregation-options` clause by now.
         _
         (throw (ex-info (tru "Expected :aggregation-options clause, got {0}" (pr-str ag))
                         {:type qp.error-type/qp, :clause ag}))))
     aggregation)
    fields]))

(defn- add-alias-info* [{:keys [source-table joins], aggregations :aggregation, :as inner-query}]
  (assert (not (:strategy inner-query)))
  (let [this-level-joins (into #{} (map :alias) joins)
        clause->position (selected-clauses inner-query)]
    (mbql.u/replace inner-query
      ;; don't rewrite anything inside any source queries.
      (_ :guard (constantly (contains? (set &parents) :source-query)))
      &match

      [:field (field-name :guard string?) opts]
      [:field field-name (merge opts
                                {::source-table ::source
                                 ::source-alias field-name}
                                (when-let [position (clause->position &match)]
                                  {::desired-alias field-name
                                   ::position      position}))]

      [:field (id :guard integer?) opts]
      (let [{table-id :table_id, field-name :name} (qp.store/field id)
            {:keys [join-alias]}                   opts
            join-is-this-level?                    (some-> join-alias this-level-joins)
            table                                  (cond
                                                     (= table-id source-table) table-id
                                                     join-is-this-level?       join-alias
                                                     :else                     ::source)
            source-alias                           (if (and join-alias (not join-is-this-level?))
                                                     (format "%s__%s" join-alias field-name)
                                                     field-name)
            desired-alias                          (if join-alias
                                                     (format "%s__%s" join-alias field-name)
                                                     field-name)]
        [:field id (merge opts
                          {::source-table table
                           ::source-alias source-alias}
                          (when-let [position (clause->position &match)]
                            {::desired-alias desired-alias
                             ::position      position}))])

      [:aggregation index]
      (let [position (clause->position &match)]
        (assert source-table)
        (assert position (format "Aggregation does not exist at index %d" index))
        (let [[_ ag-name _] (nth aggregations index)]
          [:aggregation index {::desired-alias ag-name
                               ::position      position}]))

      [:expression expression-name]
      (let [position (clause->position &match)]
        (assert position (format "Expression with name %s does not exist at this level" (pr-str expression-name)))
        [:expression expression-name {::desired-alias expression-name
                                      ::position      position}]))))

(defn add-alias-info [query]
  (walk/postwalk
   (fn [form]
     (if (and (map? form)
              ((some-fn :source-query :source-table) form)
              (not (:strategy form)))
       (add-alias-info* form)
       form))
   query))

(defn uniquify-aliases
  "Make sure the `::desired-alias` of all of every selected field reference is unique."
  [inner-query]
  ;; TODO
  )

;; TODO -- raise query

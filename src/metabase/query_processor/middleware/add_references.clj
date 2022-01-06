(ns metabase.query-processor.middleware.add-references
  "TODO -- consider moving this into a QP util namespace?"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metabase.mbql.schema :as mbql.s]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.middleware.add-implicit-clauses :as add-implicit-clauses]
            [metabase.query-processor.middleware.add-references :as add-references]
            [metabase.query-processor.middleware.annotate :as annotate]
            [metabase.query-processor.store :as qp.store]
            [metabase.util :as u]
            [metabase.util.i18n :refer [trs]]
            [schema.core :as s]))

(defn- remove-namespaced-options [options]
  (into {}
        (remove (fn [[k _]]
                  (when (keyword? k)
                    (namespace k))))
        options))

(defn- remove-default-temporal-unit [{:keys [temporal-unit], :as options}]
  (cond-> options
    (= temporal-unit :default)
    (dissoc :temporal-unit)))

(defn normalize-clause [clause]
  (cond-> clause
    (mbql.u/is-clause? :field clause)
    (mbql.u/update-field-options (comp remove-namespaced-options
                                       remove-default-temporal-unit))))

(defn field-ref-info [query clause]
  (let [clause (normalize-clause clause)]
    (or (get-in query [:qp/refs clause])
        ;; HACK HACK HACK HACK Ideally we shouldn't have to do any of this 'closest match' nonsense. We need to add
        ;; middleware to reconcile/fix bad references automatically
        (let [[closest-match info] (mbql.u/match-one clause
                                     [:field id-or-name _]
                                     (some (fn [[a-clause info]]
                                             (mbql.u/match-one a-clause
                                               [:field an-id-or-name _]
                                               (when (= an-id-or-name id-or-name)
                                                 [a-clause info])))
                                           (:qp/refs query)))]
          (log/warnf (str/join \newline [(trs "Missing Field ref info for {0}" (u/colorize 'red (pr-str clause)))
                                         (trs "Field refs:")
                                         (u/pprint-to-str (:qp/refs query))
                                         (trs "Using closest match {0} {1}" (u/colorize 'cyan (pr-str closest-match)) (u/colorize 'yellow info))]))
          info))))

(defn- source-table-references [source-table-id join-alias]
  (when source-table-id
    (let [field-clauses (add-implicit-clauses/sorted-implicit-fields-for-table source-table-id)
          field-ids     (into #{} (mbql.u/match field-clauses
                                    [:field (id :guard int?) _]
                                    id))]
      ;; make sure the Fields are in the store. They might not be if this is a Table from a join where we don't select
      ;; all its fields
      (qp.store/fetch-and-store-fields! field-ids)
      (into
       {}
       (comp (map normalize-clause)
             (map (fn [clause]
                    [clause (if join-alias
                              {:source {:table join-alias}}
                              {})])))
       field-clauses))))

(defn- add-alias-info [refs]
  (into
   {}
   (comp (map (fn [[clause info]]
                [(normalize-clause clause) info]))
         (map (fn [[clause info]]
                (mbql.u/match-one clause
                  [:field (id :guard integer?) _opts]
                  (let [field      (qp.store/field id)
                        field-name (:name field)]
                    [clause (assoc info :alias field-name)])

                  [:field (field-name :guard string?) _opts]
                  [clause (assoc info :alias field-name)]

                  _
                  [clause info]))))
   refs))

(defn- selected-references [{:keys [fields breakout aggregation]}]
  (into
   {}
   (comp cat
         (map normalize-clause)
         (map-indexed
          (fn [i clause]
            (mbql.u/match-one clause
              [:aggregation-options _ opts]
              [[:aggregation (::index opts)] {:position i, :alias (:name opts)}]

              [:expression expression-name]
              [[:expression expression-name] {:position i, :alias expression-name}]

              _
              [clause {:position i}]))))
   [breakout
    (map-indexed
     (fn [i ag]
       (mbql.u/replace ag
         [:aggregation-options wrapped opts]
         [:aggregation-options wrapped (assoc opts ::index i)]))
     aggregation)
    fields]))

(defn- mbql-source-query-references
  [{refs :qp/refs, aggregations :aggregation, :keys [expressions], :as source-query}]
  (into
   {}
   (comp (filter (fn [[_clause info]]
                   (:position info)))
         (map (fn [[clause info]]
                [(normalize-clause clause) info]))
         (map (fn [[clause info]]
                (let [clause (mbql.u/match-one clause
                               :field
                               &match

                               [:aggregation index]
                               (let [ag-clause              (get aggregations index)
                                     {base-type :base_type} (annotate/col-info-for-aggregation-clause source-query ag-clause)]
                                 [:field (:alias info) {:base-type (or base-type :type/*)}])

                               [:expression expression-name]
                               (let [expression-definition (get expressions (keyword expression-name))
                                     {base-type :base_type} (some-> expression-definition annotate/infer-expression-type)]
                                 [:field expression-name {:base-type (or base-type :type/*)}])

                               _
                               clause)
                      info (-> info
                               (assoc :source {:table ::source, :alias (:alias info)})
                               (dissoc :position))]
                  [clause info]))))
   refs))

(defn- native-source-query-references [source-query source-metadata]
  (when (:native source-query)
    (into {}
          (comp (map (fn [{field-name :name, base-type :base_type}]
                       [[:field field-name {:base-type base-type}] {:alias field-name
                                                                    :source {:table ::source, :alias field-name}}]))
                (map (fn [[clause info]]
                       [(normalize-clause clause) info]))
                (map-indexed (fn [i [clause info]]
                               [clause (assoc info :position i)])))
          source-metadata)))

(defn- join-references [joins]
  (into
   {}
   (comp (mapcat (fn [{join-alias :alias, refs :qp/refs}]
                   (for [[clause info] refs
                         :let          [info (assoc info
                                                    :source {:table join-alias, :alias (:alias info)}
                                                    ;; TODO -- this needs to use [[metabase.driver.sql.query-processor/prefix-field-alias]]
                                                    ;; or some new equivalent method
                                                    :alias (format "%s__%s" join-alias (or (get-in info [:source :alias])
                                                                                           (:alias info))))]]
                     (mbql.u/match-one clause
                       [:field id-or-name opts] [[:field id-or-name (assoc opts :join-alias join-alias)] info]
                       _                        [clause info]))))
         (map (fn [[clause info]]
                [(normalize-clause clause) info])))
   joins))

(defn- uniquify-visible-ref-aliases [refs]
  (let [unique-name (mbql.u/unique-name-generator)]
    (into
     {}
     (map (fn [[clause info]]
            [clause (cond-> info
                      (:position info) (update :alias unique-name))]))
     refs)))

(defn- recursive-merge [& maps]
  (if (every? (some-fn nil? map?) maps)
    (apply merge-with recursive-merge maps)
    (last maps)))

(def ^:private QPRefs
  ;; TODO -- ensure unique `:position` ?
  ;; NOCOMMIT
  {mbql.s/FieldOrAggregationReference s/Any #_{:alias                     su/NonBlankString
                                               (s/optional-key :position) s/Int
                                               (s/optional-key :source)   {:table su/NonBlankString
                                                                           :alias su/NonBlankString}}})

(s/defn ^:private add-references* :- {s/Keyword s/Any
                                      :qp/refs QPRefs}
  [{:keys [source-table source-query source-metadata joins]
    join-alias :alias
    :as inner-query}]
  (let [refs (-> (recursive-merge
                  (add-alias-info (source-table-references source-table join-alias))
                  (add-alias-info (selected-references inner-query))
                  (mbql-source-query-references source-query)
                  (native-source-query-references source-query source-metadata)
                  (join-references joins))
                 uniquify-visible-ref-aliases)]
    (assoc inner-query :qp/refs refs)))

(defn- remove-join-references [x]
  (mbql.u/replace x
    (m :guard (every-pred map? :alias :qp/refs))
    (remove-join-references (dissoc m :qp/refs))))

(defn add-references [query]
  (let [query (walk/postwalk
               (fn [form]
                 (if (and (map? form)
                          ((some-fn :source-query :source-table) form))
                   (add-references* form)
                   form))
               query)]
    (remove-join-references query)))

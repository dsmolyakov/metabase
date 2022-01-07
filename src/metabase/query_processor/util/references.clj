(ns metabase.query-processor.util.references
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metabase.mbql.schema :as mbql.s]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.error-type :as qp.error-type]
            [metabase.query-processor.middleware.add-implicit-clauses :as add-implicit-clauses]
            [metabase.query-processor.middleware.annotate :as annotate]
            [metabase.query-processor.store :as qp.store]
            [metabase.util :as u]
            [metabase.util.i18n :refer [trs tru]]
            [metabase.util.schema :as su]
            [schema.core :as s]))

(def ^:private QPRefs
  (-> {mbql.s/FieldOrAggregationReference {:alias                     su/NonBlankString
                                           (s/optional-key :position) s/Int
                                           (s/optional-key :source)   {:table (s/cond-pre (s/eq ::source)
                                                                                          su/NonBlankString)
                                                                       :alias su/NonBlankString}}}
      (s/constrained (fn [m]
                       (let [positions (->> m
                                            (map (fn [[_clause {:keys [position]}]]
                                                   position))
                                            (filter some?))]
                         (or (empty? positions)
                             (and (apply distinct? positions)
                                  (= (sort positions) (range 0 (inc (reduce max positions))))))))
                     "all :positions must be sequential, and unique.")))

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

(defn normalize-clause
  "Normalize a `:field`/`:expression`/`:aggregation` clause by removing extra info so it can serve as a key for
  `:qp/refs`."
  [clause]
  (cond-> clause
    (mbql.u/is-clause? :field clause)
    (mbql.u/update-field-options (comp remove-namespaced-options
                                       remove-default-temporal-unit))))

(defn field-ref-info
  "Get the matching `:qp/refs` info for a `:field`/`:expression`/`:aggregation` clause in `query`."
  [query clause]
  (let [clause (normalize-clause clause)]
    (or (get-in query [:qp/refs clause])
        ;; TODO -- not sure whether it would make sense to also check if there's a match without binning before doing the 'closest' match thing.
        ;;
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
                    [clause (cond-> (assoc info :alias field-name)
                              (:source info) (assoc-in [:source :alias] field-name))])

                  [:field (field-name :guard string?) _opts]
                  [clause (assoc info
                                 :alias field-name
                                 :source {:table ::source, :alias field-name})]

                  _
                  [clause info]))))
   refs))

(defn- expression-references [{:keys [expressions]}]
  (into
   {}
   (comp (map (fn [[expression-name expression-definition]]
                (let [expression-name (u/qualified-name expression-name)]
                  [[:expression expression-name]
                   {:alias expression-name}])))
         (map (fn [[clause info]]
                [(normalize-clause clause) info])))
   expressions))

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
         [:aggregation-options wrapped (assoc opts ::index i)]

         ;; aggregation clause should be preprocessed into an `:aggregation-options` clause by now.
         _
         (throw (ex-info (tru "Expected :aggregation-options clause, got {0}" (pr-str ag))
                         {:type qp.error-type/qp, :clause ag}))))
     aggregation)
    fields]))

(defn- raise-source-query-aggregation-ref
  "Convert an `:aggregation` reference from a source query into an appropriate `:field` clause for use in the
  surrounding query.

    (raise-source-query-aggregation-ref {:aggregation [[:count]]} 0) ; -> [:field \"count\" {:base-type :type/Integer}]"
  [{aggregations :aggregation, :as source-query} index ref-info]
  (let [ag-clause              (or (get aggregations index)
                                   (throw (ex-info (tru "No aggregation at index {0}" index)
                                                   {:type  qp.error-type/invalid-query
                                                    :index index
                                                    :query source-query})))
        {base-type :base_type} (annotate/col-info-for-aggregation-clause source-query ag-clause)]
    [:field (:alias ref-info) {:base-type (or base-type :type/*)}]))

(defn- raise-source-query-expression-ref
  "Convert an `:expression` reference from a source query into an appropriate `:field` clause for use in the surrounding
  query."
  [{:keys [expressions], :as source-query} expression-name]
  (let [expression-definition (or (get expressions (keyword expression-name))
                                  (throw (ex-info (tru "No expression named {0}" (pr-str expression-name))
                                                  {:type            qp.error-type/invalid-query
                                                   :expression-name expression-name
                                                   :query           source-query})))
        {base-type :base_type} (some-> expression-definition annotate/infer-expression-type)]
    [:field expression-name {:base-type (or base-type :type/*)}]))

(defn raise-source-query-field-or-ref
  "Convert a `:field`/`:aggregation` reference/`:expression` reference/etc. to an `:field` clause for use in the
  surrounding query."
  ([source-query clause]
   (raise-source-query-field-or-ref source-query clause (field-ref-info source-query clause)))

  ([source-query clause ref-info]
   (assert ref-info)                    ; NOCOMMIT -- convert to `tru` or something
   (mbql.u/match-one clause
     [:field (id :guard integer?) (_opts :guard :join-alias)]
     (let [{field-alias :alias}   ref-info
           {base-type :base_type} (qp.store/field id)]
       [:field field-alias {:base-type base-type}])

     [:field (field-name :guard string?) (opts :guard :join-alias)]
     (let [{field-alias :alias} ref-info]
       [:field field-alias (select-keys opts [:base-type])])

     :field
     clause

     [:aggregation index]
     (raise-source-query-aggregation-ref source-query index ref-info)

     [:expression expression-name]
     (raise-source-query-expression-ref source-query expression-name)

     _
     (throw (ex-info (tru "Don''t know how to raise {0}" (pr-str clause))
                     {:clause clause})))))

(defn- mbql-source-query-references
  [{refs :qp/refs, :as source-query}]
  (into
   {}
   (comp (filter (fn [[_clause info]]
                   (:position info)))
         (map (fn [[clause info]]
                [(normalize-clause clause) info]))
         (map (fn [[clause info]]
                (let [clause (raise-source-query-field-or-ref source-query clause info)
                      info   (-> info
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

(defn- source-query-references [source-query source-metadata]
  (if (:native source-query)
    (native-source-query-references source-query source-metadata)
    (mbql-source-query-references source-query)))

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
                [(normalize-clause clause) (dissoc info :position)])))
   joins))

(defn- uniquify-selected-references-aliases [refs]
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

(s/defn ^:private add-references* :- {s/Keyword s/Any
                                      :qp/refs QPRefs}
  [{:keys [source-table source-query source-metadata joins]
    join-alias :alias
    :as inner-query}]
  (let [refs (-> (recursive-merge
                  (add-alias-info (source-table-references source-table join-alias))
                  (add-alias-info (expression-references inner-query))
                  (add-alias-info (selected-references inner-query))
                  (source-query-references source-query source-metadata)
                  (join-references joins))
                 uniquify-selected-references-aliases)]
    (assoc inner-query :qp/refs refs)))

(defn- remove-join-references
  "Remove `:qp/refs` key from the top level of a join map (these are not needed, and should not be used for things like
  compiling join conditions -- use the parent query refs instead)."
  [x]
  (mbql.u/replace x
    (m :guard (every-pred map? :alias :qp/refs))
    (remove-join-references (dissoc m :qp/refs))))

(defn add-references
  "Walk `query` and add `:qp/refs` keys at all levels. `:qp/refs` is a map of 'visible' Field/aggregation
  reference/expression reference -> alias info for that level of the query. Use [[field-ref-info]] to get the matching
  info for a given Field clause."
  [query]
  (let [query (walk/postwalk
               (fn [form]
                 (if (and (map? form)
                          ((some-fn :source-query :source-table) form))
                   (add-references* form)
                   form))
               query)]
    (remove-join-references query)))

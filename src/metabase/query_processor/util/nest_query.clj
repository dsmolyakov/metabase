(ns metabase.query-processor.util.nest-query
  "Utility functions for raising/nesting parts of MBQL queries. Currently, this only has [[nest-expressions]], but in
  the future hopefully we can generalize this a bit so we can do more things that require us to introduce another
  level of nesting, e.g. support window functions.

   (This namespace is here rather than in the shared MBQL lib because it relies on other QP-land utils like the QP
  refs stuff.)"
  (:require [metabase.mbql.util :as mbql.u]
            [metabase.query-processor :as qp]
            [metabase.query-processor.store :as qp.store]
            [metabase.query-processor.util.references :as refs]
            [metabase.util :as u]))

(defn- ensure-refs [{:qp/keys [refs], :as query}]
  (cond-> query
    (not refs) refs/add-references))

(defn- nest-source [inner-query]
  (let [source (as-> (select-keys inner-query [:source-table :source-query :source-metadata :joins :expressions]) source
                 (qp/query->preprocessed {:database (u/the-id (qp.store/database))
                                          :type     :query
                                          :query    source})
                 (:query source)
                 (dissoc source :limit))]
    (-> inner-query
        (dissoc :source-table :source-metadata :joins)
        (assoc :source-query source))))

(defn- rewrite-expressions-and-joined-fields-as-field-literals [query]
  (mbql.u/replace query
    [:expression expression-name]
    (refs/raise-source-query-field-or-ref query &match)

    [:field id-or-name (opts :guard :join-alias)]
    (let [{field-alias :alias} (refs/field-ref-info query &match)]
      (assert field-alias)
      [:field field-alias {:base-type :type/Integer}])

    ;; when recursing into joins use the refs from the parent level.
    (m :guard (every-pred map? :joins))
    (let [{:keys [joins]} m]
      (-> (dissoc m :joins)
          rewrite-expressions-and-joined-fields-as-field-literals
          (assoc :joins (mapv (fn [join]
                                (assoc join :qp/refs (:qp/refs query)))
                              joins))))

    ;; don't recurse into any `:source-query` maps.
    (m :guard (every-pred map? :source-query))
    (let [{:keys [source-query]} m]
      (-> (dissoc m :source-query)
          rewrite-expressions-and-joined-fields-as-field-literals
          (assoc :source-query source-query)))))

(defn nest-expressions
  "Pushes the `:source-table`/`:source-query`, `:expressions`, and `:joins` in the top-level of the query into a
  `:source-query` and updates `:expression` references and `:field` clauses with `:join-alias`es accordingly. See
  tests for examples. This is used by the SQL QP to make sure expressions happen in a subselect."
  [{:keys [expressions], :as query}]
  (if (empty? expressions)
    query
    (let [query                               (-> query
                                                  ensure-refs
                                                  rewrite-expressions-and-joined-fields-as-field-literals)
          {:keys [source-query], :as query}   (nest-source query)
          source-query                        (assoc source-query :expressions expressions)
          {:qp/keys [refs], :as source-query} (refs/add-references source-query)]
      (-> query
          (dissoc :source-query :expressions)
          (assoc :source-query source-query)
          ;; update references.
          refs/add-references))))

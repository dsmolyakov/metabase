(ns metabase.query-processor.util.add-alias-info-test
  (:require [metabase.query-processor.util.add-alias-info :as add]
            [clojure.test :refer :all]
            [metabase.query-processor :as qp]
            [metabase.test :as mt]
            [clojure.walk :as walk]))

(defn- remove-source-metadata [x]
  (walk/postwalk
   (fn [x]
     (if ((every-pred map? :source-metadata) x)
       (dissoc x :source-metadata)
       x))
   x))

(defn- add-alias-info [query]
  (mt/with-everything-store
    (-> query qp/query->preprocessed add/add-alias-info remove-source-metadata (dissoc :middleware))))

(deftest join-in-source-query-test
  (is (query= (mt/mbql-query venues
                {:source-query {:source-table $$venues
                                :joins        [{:strategy     :left-join
                                                :source-table $$categories
                                                :alias        "Cat"
                                                :condition    [:=
                                                               [:field %category_id {::add/source-table  $$venues
                                                                                     ::add/source-alias  "CATEGORY_ID"}]
                                                               [:field %categories.id {:join-alias         "Cat"
                                                                                       ::add/source-table  "Cat"
                                                                                       ::add/source-alias  "ID"}]]}]
                                :fields       [[:field %id {::add/source-table  $$venues
                                                            ::add/source-alias  "ID"
                                                            ::add/desired-alias "ID"
                                                            ::add/position      0}]
                                               [:field %categories.name {:join-alias         "Cat"
                                                                         ::add/source-table  "Cat"
                                                                         ::add/source-alias  "NAME"
                                                                         ::add/desired-alias "Cat__NAME"
                                                                         ::add/position      1}]]}
                 :breakout     [[:field %categories.name {:join-alias         "Cat"
                                                          ::add/source-table  ::add/source
                                                          ::add/source-alias  "Cat__NAME"
                                                          ::add/desired-alias "Cat__NAME"
                                                          ::add/position      0}]]
                 :order-by     [[:asc [:field %categories.name {:join-alias         "Cat"
                                                                ::add/source-table  ::add/source
                                                                ::add/source-alias  "Cat__NAME"
                                                                ::add/desired-alias "Cat__NAME"
                                                                ::add/position      0}]]]
                 :limit        1})
              (add-alias-info
               (mt/mbql-query venues
                 {:source-query {:source-table $$venues
                                 :joins        [{:strategy     :left-join
                                                 :source-table $$categories
                                                 :alias        "Cat"
                                                 :condition    [:= $category_id &Cat.categories.id]}]
                                 :fields       [$id
                                                &Cat.categories.name]}
                  :breakout     [&Cat.categories.name]
                  :limit        1})))))

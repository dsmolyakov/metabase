(ns metabase.query-processor.util.nest-query-test
  (:require [clojure.test :refer :all]
            [clojure.walk :as walk]
            [metabase.driver :as driver]
            [metabase.query-processor :as qp]
            [metabase.query-processor.util.nest-query :as nest-query]
            [metabase.query-processor.util.references :as refs]
            [metabase.test :as mt]))

(defn- remove-source-metadata [x]
  (walk/postwalk
   (fn [x]
     (if ((every-pred map? :source-metadata) x)
       (dissoc x :source-metadata)
       x))
   x))

(defn- nest-expressions [query]
  (mt/with-everything-store
    (driver/with-driver :h2
      (-> query
          qp/query->preprocessed
          :query
          nest-query/nest-expressions
          remove-source-metadata))))

(deftest nest-expressions-test
  (is (query= (mt/$ids venues
                {:source-query {:source-table $$venues
                                :expressions  {:double_price [:* $price 2]}
                                :fields       [$id
                                               $name
                                               $category_id
                                               $latitude
                                               $longitude
                                               $price
                                               [:expression "double_price"]]
                                :qp/refs      {$category_id                 {:alias "CATEGORY_ID", :position 2}
                                               $id                          {:alias "ID", :position 0}
                                               $latitude                    {:alias "LATITUDE", :position 3}
                                               $longitude                   {:alias "LONGITUDE", :position 4}
                                               $name                        {:alias "NAME", :position 1}
                                               $price                       {:alias "PRICE", :position 5}
                                               [:expression "double_price"] {:alias "double_price", :position 6}}}
                 :breakout     [$price]
                 :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                 :fields       [*double_price/Float]
                 :order-by     [[:asc $price]]
                 :qp/refs      {$category_id        {:alias "CATEGORY_ID", :source {:alias "CATEGORY_ID", :table ::refs/source}}
                                $id                 {:alias "ID", :source {:alias "ID", :table ::refs/source}}
                                $latitude           {:alias "LATITUDE", :source {:alias "LATITUDE", :table ::refs/source}}
                                $longitude          {:alias "LONGITUDE", :source {:alias "LONGITUDE", :table ::refs/source}}
                                $name               {:alias "NAME", :source {:alias "NAME", :table ::refs/source}}
                                $price              {:alias "PRICE", :position 0, :source {:alias "PRICE", :table ::refs/source}}
                                *double_price/Float {:alias "double_price", :position 2, :source {:alias "double_price", :table ::refs/source}}
                                [:aggregation 0]    {:alias "count", :position 1}}})
              (nest-expressions
               (mt/mbql-query venues
                 {:expressions {:double_price [:* $price 2]}
                  :breakout    [$price]
                  :aggregation [[:count]]
                  :fields      [[:expression "double_price"]]})))))

(deftest nest-expressions-test-2
  (is (query= (mt/$ids venues
                {:source-query {:source-table $$checkins
                                :expressions  {:double_id [:* $checkins.id 2]}
                                :fields       [$checkins.id
                                               !default.checkins.date
                                               $checkins.user_id
                                               $checkins.venue_id
                                               [:expression "double_id"]]
                                :qp/refs      {$checkins.date            {:alias "DATE", :position 1}
                                               $checkins.id              {:alias "ID", :position 0}
                                               $checkins.user_id         {:alias "USER_ID", :position 2}
                                               $checkins.venue_id        {:alias "VENUE_ID", :position 3}
                                               [:expression "double_id"] {:alias "double_id", :position 4}}}
                 :fields       [*double_id/Float
                                !day.checkins.date
                                !month.checkins.date]
                 :limit        1
                 :qp/refs      {!day.checkins.date   {:alias "DATE", :position 1}
                                !month.checkins.date {:alias "DATE_2", :position 2}
                                $checkins.date       {:alias "DATE", :source {:alias "DATE", :table ::refs/source}}
                                $checkins.id         {:alias "ID", :source {:alias "ID", :table ::refs/source}}
                                $checkins.user_id    {:alias "USER_ID", :source {:alias "USER_ID", :table ::refs/source}}
                                $checkins.venue_id   {:alias "VENUE_ID", :source {:alias "VENUE_ID", :table ::refs/source}}
                                *double_id/Float     {:alias "double_id", :position 0, :source {:alias "double_id", :table ::refs/source}}}})
              (nest-expressions
               (mt/mbql-query checkins
                 {:expressions {:double_id [:* $id 2]}
                  :fields      [[:expression "double_id"]
                                !day.date
                                !month.date]
                  :limit       1})))))

(deftest nest-expressions-ignore-source-queries-test
  (testing "When 'raising' :expression clauses, only raise ones in the current level. Handle duplicate expression names correctly."
    (is (query= (mt/$ids venues
                  {:source-query
                   {:source-query
                    {:source-table $$venues
                     :expressions  {:x [:* $price 2]}
                     :fields       [$id [:expression "x"]]
                     :qp/refs      {$category_id      {:alias "CATEGORY_ID"}
                                    $id               {:alias "ID", :position 0}
                                    $latitude         {:alias "LATITUDE"}
                                    $longitude        {:alias "LONGITUDE"}
                                    $name             {:alias "NAME"}
                                    $price            {:alias "PRICE"}
                                    [:expression "x"] {:alias "x", :position 1}}}
                    :expressions {:x [:* $price 4]}
                    :fields      [$id *x/Float [:expression "x"]]
                    :qp/refs     {$id               {:alias "ID", :position 0, :source {:alias "ID", :table ::refs/source}}
                                  *x/Float          {:alias "x_2", :position 1, :source {:alias "x", :table ::refs/source}}
                                  [:expression "x"] {:alias "x", :position 2}}}
                   :fields  [$id *x/Float]
                   :limit   1
                   :qp/refs {$id      {:alias "ID", :position 0, :source {:alias "ID", :table ::refs/source}}
                             *x/Float {:alias "x_2", :position 1, :source {:alias "x_2", :table ::refs/source}}}})
                (nest-expressions
                 (mt/mbql-query venues
                   {:source-query {:source-table $$venues
                                   :expressions  {:x [:* $price 2]}
                                   :fields       [$id [:expression "x"]]}
                    :expressions  {:x [:* $price 4]}
                    :fields       [$id [:expression "x"]]
                    :limit        1}))))))

#_(defn- remove-qp-refs [x]
  (walk/postwalk
   (fn [x]
     (if ((every-pred map? :qp/refs) x)
       (dissoc x :qp/refs)
       x))
   x))

(defn y []
  (qp/query->preprocessed (mt/mbql-query venues
                            {:joins       [{:strategy     :left-join
                                            :condition    [:= $category_id &CategoriesStats.category_id]
                                            :source-query {:source-table $$venues
                                                           :aggregation  [[:aggregation-options [:max $price] {:name "MaxPrice"}]
                                                                          [:aggregation-options [:avg $price] {:name "AvgPrice"}]
                                                                          [:aggregation-options [:min $price] {:name "MinPrice"}]]
                                                           :breakout     [$category_id]
                                                           :order-by     [[:asc $category_id]]}
                                            :alias        "CategoriesStats"
                                            :fields       [&CategoriesStats.category_id
                                                           &CategoriesStats.*MaxPrice/Integer
                                                           &CategoriesStats.*AvgPrice/Integer
                                                           &CategoriesStats.*MinPrice/Integer]}]
                             :expressions {:RelativePrice [:/ $price &CategoriesStats.*AvgPrice/Integer]}})))

(deftest nest-expressions-with-joins-test
  (testing "If there are any `:joins`, those need to be nested into the `:source-query` as well."
    (is (query= (mt/$ids venues
                  {:source-query {:source-table $$venues
                                  :joins        [{:strategy     :left-join
                                                  :condition    [:= $category_id &CategoriesStats.category_id]
                                                  :source-query {:source-table $$venues
                                                                 :aggregation  [[:aggregation-options [:max $price] {:name "MaxPrice"}]
                                                                                [:aggregation-options [:avg $price] {:name "AvgPrice"}]
                                                                                [:aggregation-options [:min $price] {:name "MinPrice"}]]
                                                                 :breakout     [$category_id]
                                                                 :order-by     [[:asc $category_id]]
                                                                 :qp/refs      {$category_id     {:alias "CATEGORY_ID", :position 0}
                                                                                $id              {:alias "ID"}
                                                                                $latitude        {:alias "LATITUDE"}
                                                                                $longitude       {:alias "LONGITUDE"}
                                                                                $name            {:alias "NAME"}
                                                                                $price           {:alias "PRICE"}
                                                                                [:aggregation 0] {:alias "MaxPrice", :position 1}
                                                                                [:aggregation 1] {:alias "AvgPrice", :position 2}
                                                                                [:aggregation 2] {:alias "MinPrice", :position 3}}}
                                                  :alias        "CategoriesStats"
                                                  :fields       [&CategoriesStats.category_id
                                                                 &CategoriesStats.*MaxPrice/Integer
                                                                 &CategoriesStats.*AvgPrice/Integer
                                                                 &CategoriesStats.*MinPrice/Integer]}]
                                  :expressions  {:RelativePrice [:/ $price &CategoriesStats.*AvgPrice/Integer]}
                                  :fields       [$id
                                                 $name
                                                 $category_id
                                                 $latitude
                                                 $longitude
                                                 $price
                                                 [:expression "RelativePrice"]
                                                 &CategoriesStats.category_id
                                                 &CategoriesStats.*MaxPrice/Integer
                                                 &CategoriesStats.*AvgPrice/Integer
                                                 &CategoriesStats.*MinPrice/Integer]
                                  :qp/refs      {$category_id                       {:alias "CATEGORY_ID", :position 2}
                                                 $id                                {:alias "ID", :position 0}
                                                 $latitude                          {:alias "LATITUDE", :position 3}
                                                 $longitude                         {:alias "LONGITUDE", :position 4}
                                                 $name                              {:alias "NAME", :position 1}
                                                 $price                             {:alias "PRICE", :position 5}
                                                 [:expression "RelativePrice"]      {:alias "RelativePrice", :position 6}
                                                 &CategoriesStats.category_id       {:alias "CategoriesStats__CATEGORY_ID", :position 7, :source {:alias "CATEGORY_ID", :table "CategoriesStats"}}
                                                 &CategoriesStats.*MaxPrice/Integer {:alias "CategoriesStats__MaxPrice", :position 8, :source {:alias "MaxPrice", :table "CategoriesStats"}}
                                                 &CategoriesStats.*AvgPrice/Integer {:alias "CategoriesStats__AvgPrice", :position 9, :source {:alias "AvgPrice", :table "CategoriesStats"}}
                                                 &CategoriesStats.*MinPrice/Integer {:alias "CategoriesStats__MinPrice", :position 10, :source {:alias "MinPrice", :table "CategoriesStats"}}}}
                   :fields       [$id
                                  $name
                                  $category_id
                                  $latitude
                                  $longitude
                                  $price
                                  *RelativePrice/Float
                                  *CategoriesStats__CATEGORY_ID/Integer
                                  *CategoriesStats__MaxPrice/Integer
                                  *CategoriesStats__AvgPrice/Integer
                                  *CategoriesStats__MinPrice/Integer]
                   :limit        3
                   :qp/refs      {$venues.category_id                   {:alias "CATEGORY_ID", :position 2, :source {:alias "CATEGORY_ID", :table ::refs/source}},
                                  $venues.id                            {:alias "ID", :position 0, :source {:alias "ID", :table ::refs/source}},
                                  $venues.latitude                      {:alias "LATITUDE", :position 3, :source {:alias "LATITUDE", :table ::refs/source}},
                                  $venues.longitude                     {:alias "LONGITUDE", :position 4, :source {:alias "LONGITUDE", :table ::refs/source}},
                                  $venues.name                          {:alias "NAME", :position 1, :source {:alias "NAME", :table ::refs/source}},
                                  $venues.price                         {:alias "PRICE", :position 5, :source {:alias "PRICE", :table ::refs/source}},
                                  *CategoriesStats__AvgPrice/Integer    {:alias "CategoriesStats__AvgPrice", :position 9, :source {:alias "CategoriesStats__AvgPrice", :table ::refs/source}},
                                  *CategoriesStats__MaxPrice/Integer    {:alias "CategoriesStats__MaxPrice", :position 8, :source {:alias "CategoriesStats__MaxPrice", :table ::refs/source}},
                                  *CategoriesStats__MinPrice/Integer    {:alias "CategoriesStats__MinPrice", :position 10, :source {:alias "CategoriesStats__MinPrice", :table ::refs/source}},
                                  *CategoriesStats__CATEGORY_ID/Integer {:alias "CategoriesStats__CATEGORY_ID", :position 7, :source {:alias "CategoriesStats__CATEGORY_ID", :table ::refs/source}},
                                  *RelativePrice/Float                  {:alias "RelativePrice", :position 6, :source {:alias "RelativePrice", :table ::refs/source}}}})
                (nest-expressions
                 (mt/mbql-query venues
                   {:fields      [$id
                                  $name
                                  $category_id
                                  $latitude
                                  $longitude
                                  $price
                                  [:expression "RelativePrice"]
                                  &CategoriesStats.category_id
                                  &CategoriesStats.*MaxPrice/Integer
                                  &CategoriesStats.*AvgPrice/Integer
                                  &CategoriesStats.*MinPrice/Integer]
                    :expressions {:RelativePrice [:/ $price &CategoriesStats.*AvgPrice/Integer]}
                    :joins       [{:strategy     :left-join
                                   :condition    [:= $category_id &CategoriesStats.category_id]
                                   :source-query {:source-table $$venues
                                                  :aggregation  [[:aggregation-options [:max $price] {:name "MaxPrice"}]
                                                                 [:aggregation-options [:avg $price] {:name "AvgPrice"}]
                                                                 [:aggregation-options [:min $price] {:name "MinPrice"}]]
                                                  :breakout     [$category_id]}
                                   :alias        "CategoriesStats"
                                   :fields       :all}]
                    :limit       3}))))))

(ns metabase.query-processor.util.nest-query-test
  (:require [clojure.test :refer :all]
            [clojure.walk :as walk]
            [metabase.driver :as driver]
            [metabase.models.field :refer [Field]]
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
                                  &CategoriesStats.category_id
                                  &CategoriesStats.*MaxPrice/Integer
                                  &CategoriesStats.*AvgPrice/Integer
                                  &CategoriesStats.*MinPrice/Integer]
                   :limit        3
                   :qp/refs      {$venues.category_id                {:alias "CATEGORY_ID", :position 2, :source {:alias "CATEGORY_ID", :table ::refs/source}}
                                  $venues.id                         {:alias "ID", :position 0, :source {:alias "ID", :table ::refs/source}}
                                  $venues.latitude                   {:alias "LATITUDE", :position 3, :source {:alias "LATITUDE", :table ::refs/source}}
                                  $venues.longitude                  {:alias "LONGITUDE", :position 4, :source {:alias "LONGITUDE", :table ::refs/source}}
                                  $venues.name                       {:alias "NAME", :position 1, :source {:alias "NAME", :table ::refs/source}}
                                  $venues.price                      {:alias "PRICE", :position 5, :source {:alias "PRICE", :table ::refs/source}}
                                  &CategoriesStats.*AvgPrice/Integer {:alias "CategoriesStats__AvgPrice", :position 9, :source {:alias "CategoriesStats__AvgPrice", :table ::refs/source}}
                                  &CategoriesStats.*MaxPrice/Integer {:alias "CategoriesStats__MaxPrice", :position 8, :source {:alias "CategoriesStats__MaxPrice", :table ::refs/source}}
                                  &CategoriesStats.*MinPrice/Integer {:alias "CategoriesStats__MinPrice", :position 10, :source {:alias "CategoriesStats__MinPrice", :table ::refs/source}}
                                  &CategoriesStats.category_id       {:alias "CategoriesStats__CATEGORY_ID", :position 7, :source {:alias "CategoriesStats__CATEGORY_ID", :table ::refs/source}}
                                  *RelativePrice/Float               {:alias "RelativePrice", :position 6, :source {:alias "RelativePrice", :table ::refs/source}}}})
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

(deftest nest-expressions-eliminate-duplicate-coercion-test
  (testing "If coercion happens in the source query, don't do it a second time in the parent query (#12430)"
    (mt/with-temp-vals-in-db Field (mt/id :venues :price) {:coercion_strategy :Coercion/UNIXSeconds->DateTime
                                                           :effective_type    :type/DateTime}
      (is (query= (mt/$ids venues
                      {:source-query {:source-table $$venues
                                      :expressions  {:test [:* 1 1]}
                                      :fields       [$id
                                                     $name
                                                     $category_id
                                                     $latitude
                                                     $longitude
                                                     !default.price
                                                     [:expression "test"]]
                                      :qp/refs      {$id                  {:position 0, :alias "ID"}
                                                     $name                {:position 1, :alias "NAME"}
                                                     $category_id         {:position 2, :alias "CATEGORY_ID"}
                                                     $latitude            {:position 3, :alias "LATITUDE"}
                                                     $longitude           {:position 4, :alias "LONGITUDE"}
                                                     $price               {:position 5, :alias "PRICE"}
                                                     [:expression "test"] {:position 6, :alias "test"}}}
                       :fields       [[:field %price {:temporal-unit :default, ::nest-query/outer-select true}]
                                      [:field "test" {:base-type :type/Float}]]
                       :limit        1
                       :qp/refs      {$price                                   {:position 0, :alias "PRICE", :source {:table ::refs/source, :alias "PRICE"}}
                                      [:field "test" {:base-type :type/Float}] {:position 1, :alias "test", :source {:table ::refs/source, :alias "test"}}
                                      $id                                      {:alias "ID", :source {:table ::refs/source, :alias "ID"}}
                                      $name                                    {:alias "NAME", :source {:table ::refs/source, :alias "NAME"}}
                                      $category_id                             {:alias "CATEGORY_ID", :source {:table ::refs/source, :alias "CATEGORY_ID"}}
                                      $latitude                                {:alias "LATITUDE", :source {:table ::refs/source, :alias "LATITUDE"}}
                                      $longitude                               {:alias "LONGITUDE", :source {:table ::refs/source, :alias "LONGITUDE"}}}})
                  (nest-expressions
                   (mt/mbql-query venues
                     {:expressions {:test ["*" 1 1]}
                      :fields      [$price
                                    [:expression "test"]]
                      :limit       1})))))))

(deftest multiple-joins-with-expressions-test
  (testing "We should be able to compile a complicated query with multiple joins and expressions correctly"
    (mt/dataset sample-dataset
      (is (query= (mt/$ids orders
                    {:source-query {:source-table $$orders
                                    :joins        [{:source-table $$products
                                                    :alias        "PRODUCTS__via__PRODUCT_ID"
                                                    :condition    [:= $product_id &PRODUCTS__via__PRODUCT_ID.products.id]
                                                    :strategy     :left-join
                                                    :fk-field-id  %product_id}]
                                    :expressions  {:pivot-grouping [:abs 0]}
                                    :fields       [$id
                                                   $user_id
                                                   $product_id
                                                   $subtotal
                                                   $tax
                                                   $total
                                                   $discount
                                                   !default.created_at
                                                   $quantity
                                                   [:expression "pivot-grouping"]]
                                    :qp/refs      {$created_at                                    {:alias "CREATED_AT", :position 7}
                                                   $discount                                      {:alias "DISCOUNT", :position 6}
                                                   $id                                            {:alias "ID", :position 0}
                                                   $product_id                                    {:alias "PRODUCT_ID", :position 2}
                                                   $quantity                                      {:alias "QUANTITY", :position 8}
                                                   $subtotal                                      {:alias "SUBTOTAL", :position 3}
                                                   $tax                                           {:alias "TAX", :position 4}
                                                   $total                                         {:alias "TOTAL", :position 5}
                                                   $user_id                                       {:alias "USER_ID", :position 1}
                                                   &PRODUCTS__via__PRODUCT_ID.products.category   {:alias "PRODUCTS__via__PRODUCT_ID__CATEGORY" :source {:alias "CATEGORY", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   &PRODUCTS__via__PRODUCT_ID.products.created_at {:alias "PRODUCTS__via__PRODUCT_ID__CREATED_AT" :source {:alias "CREATED_AT", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   &PRODUCTS__via__PRODUCT_ID.products.ean        {:alias "PRODUCTS__via__PRODUCT_ID__EAN" :source {:alias "EAN", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   &PRODUCTS__via__PRODUCT_ID.products.id         {:alias "PRODUCTS__via__PRODUCT_ID__ID" :source {:alias "ID", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   &PRODUCTS__via__PRODUCT_ID.products.price      {:alias "PRODUCTS__via__PRODUCT_ID__PRICE" :source {:alias "PRICE", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   &PRODUCTS__via__PRODUCT_ID.products.rating     {:alias "PRODUCTS__via__PRODUCT_ID__RATING" :source {:alias "RATING", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   &PRODUCTS__via__PRODUCT_ID.products.title      {:alias "PRODUCTS__via__PRODUCT_ID__TITLE" :source {:alias "TITLE", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   &PRODUCTS__via__PRODUCT_ID.products.vendor     {:alias "PRODUCTS__via__PRODUCT_ID__VENDOR" :source {:alias "VENDOR", :table "PRODUCTS__via__PRODUCT_ID"}}
                                                   [:expression "pivot-grouping"]                 {:alias "pivot-grouping", :position 9}}}
                     :breakout     [&PRODUCTS__via__PRODUCT_ID.products.category
                                    [:field %created_at {::nest-query/outer-select true, :temporal-unit :year}]
                                    *pivot-grouping/Float]
                     :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                     :order-by     [[:asc &PRODUCTS__via__PRODUCT_ID.products.category]
                                    [:asc [:field %created_at {::nest-query/outer-select true, :temporal-unit :year}]]
                                    [:asc *pivot-grouping/Float]]
                     :qp/refs      {!year.created_at                             {:alias "CREATED_AT", :position 1}
                                    $created_at                                  {:alias "CREATED_AT", :source {:alias "CREATED_AT", :table ::refs/source}}
                                    $discount                                    {:alias "DISCOUNT", :source {:alias "DISCOUNT", :table ::refs/source}}
                                    $id                                          {:alias "ID", :source {:alias "ID", :table ::refs/source}}
                                    $product_id                                  {:alias "PRODUCT_ID", :source {:alias "PRODUCT_ID", :table ::refs/source}}
                                    $quantity                                    {:alias "QUANTITY", :source {:alias "QUANTITY", :table ::refs/source}}
                                    $subtotal                                    {:alias "SUBTOTAL", :source {:alias "SUBTOTAL", :table ::refs/source}}
                                    $tax                                         {:alias "TAX", :source {:alias "TAX", :table ::refs/source}}
                                    $total                                       {:alias "TOTAL", :source {:alias "TOTAL", :table ::refs/source}}
                                    $user_id                                     {:alias "USER_ID", :source {:alias "USER_ID", :table ::refs/source}}
                                    &PRODUCTS__via__PRODUCT_ID.products.category {:alias "PRODUCTS__via__PRODUCT_ID__CATEGORY", :position 0, :source {:alias "PRODUCTS__via__PRODUCT_ID__CATEGORY", :table ::source}}
                                    *pivot-grouping/Float                        {:alias "pivot-grouping", :position 2, :source {:alias "pivot-grouping", :table ::refs/source}}
                                    [:aggregation 0]                             {:alias "count", :position 3}}})
                  (nest-expressions
                   (mt/mbql-query orders
                     {:aggregation [[:aggregation-options [:count] {:name "count"}]]
                      :breakout    [&PRODUCTS__via__PRODUCT_ID.products.category
                                    !year.created_at
                                    [:expression "pivot-grouping"]]
                      :expressions {:pivot-grouping [:abs 0]}
                      :order-by    [[:asc &PRODUCTS__via__PRODUCT_ID.products.category]
                                    [:asc !year.created_at]
                                    [:asc [:expression "pivot-grouping"]]]
                      :joins       [{:source-table $$products
                                     :strategy     :left-join
                                     :alias        "PRODUCTS__via__PRODUCT_ID"
                                     :fk-field-id  %product_id
                                     :condition    [:= $product_id &PRODUCTS__via__PRODUCT_ID.products.id]}]})))))))

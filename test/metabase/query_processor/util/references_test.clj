(ns metabase.query-processor.util.references-test
  (:require [clojure.test :refer :all]
            [clojure.walk :as walk]
            [dev.debug-qp :as debug-qp]
            [medley.core :as m]
            [metabase.driver :as driver]
            [metabase.models.database :refer [Database]]
            [metabase.query-processor :as qp]
            [metabase.query-processor.util.references :as refs]
            [metabase.test :as mt]
            [toucan.db :as db]))

(defn- remove-source-metadata [x]
  (walk/postwalk
   (fn [x]
     (if ((every-pred map? :source-metadata) x)
       (dissoc x :source-metadata)
       x))
   x))

(defn- add-references [{database-id :database, :as query}]
  (mt/with-everything-store
    (driver/with-driver (db/select-one-field :engine Database :id database-id)
      (-> query
          #_qp/query->preprocessed
          refs/add-references
          remove-source-metadata
          (dissoc :middleware)))))

(deftest selected-references-aggregations-test
  (testing "Make sure references to this-level aggregations work correctly"
    (is (= {[:aggregation 0]                        {:position 0, :alias "COUNT"}
            [:aggregation 1]                        {:position 1, :alias "sum_x"}
            [:field "y" {:base-type :type/Integer}] {:position 2}}
           (#'refs/selected-references
            {:aggregation [[:aggregation-options [:count] {:name "COUNT"}]
                           [:aggregation-options [:sum [:field "x" {:base-type :type/Integer}]] {:name "sum_x"}]]
             :fields      [[:field "y" {:base-type :type/Integer}]]}))))
  (testing "Make sure all aliases get uniquified"
    (is (= {:type  :query
            :query {:source-query {:native "SELECT * FROM WHATEVER"}
                    :aggregation  [[:aggregation-options [:count] {:name "COUNT"}]
                                   [:aggregation-options [:sum [:field "x" :type/Integer]] {:name "sum_x"}]]
                    :fields       [[:field "COUNT" {:base-type :type/Integer}]]
                    :qp/refs      {[:aggregation 0]                            {:position 0, :alias "COUNT"}
                                   [:aggregation 1]                            {:position 1, :alias "sum_x"}
                                   [:field "COUNT" {:base-type :type/Integer}] {:position 2, :alias "COUNT_2", :source {:table ::refs/source, :alias "COUNT"}}}}}
           (#'refs/add-references
            {:type  :query
             :query {:source-query {:native "SELECT * FROM WHATEVER"}
                     :aggregation  [[:aggregation-options [:count] {:name "COUNT"}]
                                    [:aggregation-options [:sum [:field "x" :type/Integer]] {:name "sum_x"}]]
                     :fields       [[:field "COUNT" {:base-type :type/Integer}]]}})))))

(deftest field-name-test
  (is (= {:type  :query
          :query {:source-query {}
                  :fields       [[:field "count" {:base-type :type/BigInteger}]]
                  :qp/refs      {[:field "count" {:base-type :type/BigInteger}]
                                 {:position 0
                                  :alias    "count"
                                  :source   {:table ::refs/source
                                             :alias "count"}}}}}
         (refs/add-references
          {:type  :query
           :query {:source-query {}
                   :fields       [[:field "count" {:base-type :type/BigInteger}]]}}))))

(deftest raise-source-query-field-or-ref-test
  (testing :field
    (mt/$ids venues
      (is (= $price
             (refs/raise-source-query-field-or-ref nil $price {:alias "price"})))
      ;; TODO -- not sure if this should be done here or elsewhere.
      (testing "with temporal bucketing")
      (testing "with binning")))
  (testing :expression
    (let [query (:query (mt/mbql-query venues
                          {:expressions {:double_price [:* $price 2]}}))]
      (is (= [:field "double_price" {:base-type :type/Float}]
             (refs/raise-source-query-field-or-ref query [:expression "double_price"] {:alias "double_price"})))))
  (testing :aggregation
    (driver/with-driver :h2
      (let [query (:query (mt/mbql-query venues
                            {:aggregation [[:count]]}))]
        (is (= [:field "count" {:base-type :type/BigInteger}]
               (refs/raise-source-query-field-or-ref query [:aggregation 0] {:alias "count"})))))))

(deftest expressions-refs-test
  (mt/with-everything-store
    (is (query= (mt/$ids venues
                  {$price                       {:alias "PRICE"}
                   $id                          {:alias "ID"}
                   $name                        {:alias "NAME"}
                   $category_id                 {:alias "CATEGORY_ID"}
                   $latitude                    {:alias "LATITUDE"}
                   $longitude                   {:alias "LONGITUDE"}
                   [:expression "double_price"] {:alias "double_price"}})
                (-> (refs/add-references (mt/mbql-query venues
                                           {:expressions {:double_price [:* $price 2]}}))
                    :query :qp/refs)))))

(deftest join-in-join-test
  (mt/dataset sample-dataset
    (mt/with-everything-store
      (mt/test-driver :h2
        (is (query= (mt/$ids orders
                      {$discount             {:alias "DISCOUNT"}
                       $subtotal             {:alias "SUBTOTAL"}
                       &Q2.products.category {:position 0, :alias "Q2__P2__CATEGORY", :source {:table "Q2", :alias "P2__CATEGORY"}}
                       $quantity             {:alias "QUANTITY"}
                       $user_id              {:alias "USER_ID"}
                       $total                {:alias "TOTAL"}
                       $tax                  {:alias "TAX"}
                       $id                   {:alias "ID"}
                       $created_at           {:alias "CREATED_AT"}
                       $product_id           {:alias "PRODUCT_ID"}
                       &Q2.*avg/Integer      {:alias "Q2__avg", :source {:table "Q2", :alias "avg"}}})
                    (get-in (refs/add-references
                             (mt/mbql-query orders
                               {:fields [&Q2.products.category]
                                :joins  [{:strategy     :left-join
                                          :condition    [:= $products.category &Q2.products.category]
                                          :alias        "Q2"
                                          :source-query {:source-table $$reviews
                                                         :aggregation  [[:aggregation-options
                                                                         [:avg $reviews.rating]
                                                                         {:name "avg"}]]
                                                         :breakout     [&P2.products.category]
                                                         :joins        [{:strategy     :left-join
                                                                         :source-table $$products
                                                                         :condition    [:=
                                                                                        $reviews.product_id
                                                                                        &P2.products.id]
                                                                         :alias        "P2"}]}}]}))
                            [:query :qp/refs])))))))

(deftest join-in-source-query-correct-ref-info-test
  (testing (str "It's legal to use a Field with `:join-alias` if the join happens in the source query, but the ref "
                "info should reflect that and have correct :source")
    (is (query= (mt/mbql-query venues
                  {:source-query {:source-table $$venues
                                  :joins        [{:source-table $$categories
                                                  :alias        "Cat"
                                                  :strategy     :left-join
                                                  :condition    [:= $id &Cat.categories.id]
                                                  :fields       [&Cat.categories.id]}]
                                  :fields       [$id
                                                 &Cat.categories.name]
                                  :qp/refs      {$id                  {:alias "ID", :position 0}
                                                 $name                {:alias "NAME"}
                                                 $category_id         {:alias "CATEGORY_ID"}
                                                 $latitude            {:alias "LATITUDE"}
                                                 $longitude           {:alias "LONGITUDE"}
                                                 $price               {:alias "PRICE"}
                                                 &Cat.categories.id   {:alias "Cat__ID", :source {:table "Cat", :alias "ID"}}
                                                 &Cat.categories.name {:source {:table "Cat", :alias "NAME"}, :alias "Cat__NAME", :position 1}}}
                   :fields       [$id &Cat.categories.name]
                   :qp/refs      {$id                  {:position 0, :alias "ID", :source {:table ::refs/source, :alias "ID"}}
                                  &Cat.categories.name {:position 1, :alias "Cat__NAME", :source {:table ::refs/source, :alias "Cat__NAME"}}}})
                (add-references
                 (mt/mbql-query venues
                   {:source-query {:source-table $$venues
                                   :joins        [{:source-table $$categories
                                                   :alias        "Cat"
                                                   :strategy     :left-join
                                                   :condition    [:= $id &Cat.categories.id]
                                                   :fields       [&Cat.categories.id]}]
                                   :fields       [$id
                                                  &Cat.categories.name]}
                    :fields       [$id &Cat.categories.name]}))))))

(deftest join-in-source-query-test
  (is (query= (mt/mbql-query venues
                {:source-query {:source-table $$venues
                                :joins        [{:source-table $$categories
                                                :alias        "Cat"
                                                :condition    [:= $category_id &Cat.categories.id]
                                                :strategy     :left-join
                                                :fields       [&Cat.categories.name]}]
                                :fields       [$id $name &Cat.categories.name]
                                :qp/refs      {$id                  {:alias "ID", :position 0}
                                               $name                {:alias "NAME", :position 1}
                                               $price               {:alias "PRICE"}
                                               $category_id         {:alias "CATEGORY_ID"}
                                               $latitude            {:alias "LATITUDE"}
                                               $longitude           {:alias "LONGITUDE"}
                                               &Cat.categories.name {:position 2, :alias "Cat__NAME", :source {:table "Cat", :alias "NAME"}}
                                               &Cat.categories.id   {:alias "Cat__ID", :source {:table "Cat", :alias "ID"}}}}
                 :qp/refs      {$id                  {:alias "ID", :source {:table ::refs/source, :alias "ID"}}
                                $name                {:alias "NAME", :source {:table ::refs/source, :alias "NAME"}}
                                &Cat.categories.name {:alias "Cat__NAME", :source {:table ::refs/source, :alias "Cat__NAME"}}}})
              (add-references
               (mt/mbql-query venues
                 {:source-query {:source-table $$venues
                                 :joins        [{:source-table $$categories
                                                 :alias        "Cat"
                                                 :condition    [:= $category_id &Cat.categories.id]
                                                 :strategy     :left-join
                                                 :fields       [&Cat.categories.name]}]
                                 :fields       [$id $name &Cat.categories.name]}})))))

(deftest join-against-source-query-test
  (mt/dataset sample-dataset
    (is (query= (mt/mbql-query orders
                  {:fields       [&P1.products.category]
                   :source-query {:source-table $$orders
                                  :fields       [&P1.products.category]
                                  :joins        [{:strategy     :left-join
                                                  :source-table $$products
                                                  :condition    [:= $product_id &P1.products.id]
                                                  :alias        "P1"}]
                                  :qp/refs      {$created_at             {:alias "CREATED_AT"}
                                                 $discount               {:alias "DISCOUNT"}
                                                 $id                     {:alias "ID"}
                                                 $product_id             {:alias "PRODUCT_ID"}
                                                 $quantity               {:alias "QUANTITY"}
                                                 $subtotal               {:alias "SUBTOTAL"}
                                                 $tax                    {:alias "TAX"}
                                                 $total                  {:alias "TOTAL"}
                                                 $user_id                {:alias "USER_ID"}
                                                 &P1.products.category   {:alias "P1__CATEGORY", :position 0, :source {:alias "CATEGORY", :table "P1"}}
                                                 &P1.products.created_at {:alias "P1__CREATED_AT", :source {:alias "CREATED_AT", :table "P1"}}
                                                 &P1.products.ean        {:alias "P1__EAN", :source {:alias "EAN", :table "P1"}}
                                                 &P1.products.id         {:alias "P1__ID", :source {:alias "ID", :table "P1"}}
                                                 &P1.products.price      {:alias "P1__PRICE", :source {:alias "PRICE", :table "P1"}}
                                                 &P1.products.rating     {:alias "P1__RATING", :source {:alias "RATING", :table "P1"}}
                                                 &P1.products.title      {:alias "P1__TITLE", :source {:alias "TITLE", :table "P1"}}
                                                 &P1.products.vendor     {:alias "P1__VENDOR", :source {:alias "VENDOR", :table "P1"}}}}
                   :joins        [{:strategy     :left-join
                                   :condition    [:= &P1.products.category &Q2.products.category]
                                   :alias        "Q2"
                                   :source-query {:source-table $$reviews
                                                  :fields       [&P2.products.category]
                                                  :joins        [{:strategy     :left-join
                                                                  :source-table $$products
                                                                  :condition    [:= $reviews.product_id &P2.products.id]
                                                                  :alias        "P2"}]
                                                  :qp/refs      {$reviews.body           {:alias "BODY"}
                                                                 $reviews.created_at     {:alias "CREATED_AT"}
                                                                 $reviews.id             {:alias "ID"}
                                                                 $reviews.product_id     {:alias "PRODUCT_ID"}
                                                                 $reviews.rating         {:alias "RATING"}
                                                                 $reviews.reviewer       {:alias "REVIEWER"}
                                                                 &P2.products.category   {:alias "P2__CATEGORY", :position 0, :source {:alias "CATEGORY", :table "P2"}}
                                                                 &P2.products.created_at {:alias "P2__CREATED_AT", :source {:alias "CREATED_AT", :table "P2"}}
                                                                 &P2.products.ean        {:alias "P2__EAN", :source {:alias "EAN", :table "P2"}}
                                                                 &P2.products.id         {:alias "P2__ID", :source {:alias "ID", :table "P2"}}
                                                                 &P2.products.price      {:alias "P2__PRICE", :source {:alias "PRICE", :table "P2"}}
                                                                 &P2.products.rating     {:alias "P2__RATING", :source {:alias "RATING", :table "P2"}}
                                                                 &P2.products.title      {:alias "P2__TITLE", :source {:alias "TITLE", :table "P2"}}
                                                                 &P2.products.vendor     {:alias "P2__VENDOR", :source {:alias "VENDOR", :table "P2"}}}}}]
                   :limit        1
                   :qp/refs      {&Q2.products.category {:alias "Q2__P2__CATEGORY", :source {:table "Q2", :alias "P2__CATEGORY"}}
                                  &P1.products.category {:alias "P1__CATEGORY", :position 0, :source {:table ::refs/source, :alias "P1__CATEGORY"}}}})
                (add-references
                 ;; This query is fundamentally BROKEN, we shouldn't be using `&P1.products.category` outside of the
                 ;; source query that has its join.
                 (mt/mbql-query orders
                   {:fields       [&P1.products.category]
                    :source-query {:source-table $$orders
                                   :fields       [&P1.products.category]
                                   :joins        [{:strategy     :left-join
                                                   :source-table $$products
                                                   :condition    [:= $product_id &P1.products.id]
                                                   :alias        "P1"}]}
                    :joins        [{:strategy     :left-join
                                    :condition    [:= &P1.products.category &Q2.products.category]
                                    :alias        "Q2"
                                    :source-query {:source-table $$reviews
                                                   :fields       [&P2.products.category]
                                                   :joins        [{:strategy     :left-join
                                                                   :source-table $$products
                                                                   :condition    [:= $reviews.product_id &P2.products.id]
                                                                   :alias        "P2"}]}}]
                    :limit        1}))))))

(deftest expressions-test
  (is (query= (mt/mbql-query venues
                {:expressions {"double_id" [:* $id 2]}
                 :qp/refs     {$category_id              {:alias "CATEGORY_ID"}
                               $id                       {:alias "ID"}
                               $latitude                 {:alias "LATITUDE"}
                               $longitude                {:alias "LONGITUDE"}
                               $name                     {:alias "NAME"}
                               $price                    {:alias "PRICE"}
                               [:expression "double_id"] {:alias "double_id"}}})
              (add-references
               (mt/mbql-query venues
                 {:expressions {"double_id" [:* $id 2]}}))))

  (testing ":expression in :fields"
    (is (= (mt/mbql-query venues
             {:expressions {"double_id" [:* $id 2]}
              :fields      [[:expression "double_id"]]
              :qp/refs     {$category_id              {:alias "CATEGORY_ID"}
                            $id                       {:alias "ID"}
                            $latitude                 {:alias "LATITUDE"}
                            $longitude                {:alias "LONGITUDE"}
                            $name                     {:alias "NAME"}
                            $price                    {:alias "PRICE"}
                            [:expression "double_id"] {:position 0, :alias "double_id"}}})
           (add-references
            (mt/mbql-query venues
              {:fields      [[:expression "double_id"]]
               :expressions {"double_id" [:* $id 2]}})))))

  (testing "Inside a nested query"
    (is (= (mt/mbql-query venues
             {:source-query {:source-table $$venues
                             :expressions  {:double_id [:* $id 2]}
                             :fields       [$id $name $category_id $latitude $longitude $price [:expression "double_id"]]
                             :qp/refs      {$category_id              {:position 2, :alias "CATEGORY_ID"}
                                            $id                       {:position 0, :alias "ID"}
                                            $latitude                 {:position 3, :alias "LATITUDE"}
                                            $longitude                {:position 4, :alias "LONGITUDE"}
                                            $name                     {:position 1, :alias "NAME"}
                                            $price                    {:position 5, :alias "PRICE"}
                                            [:expression "double_id"] {:position 6, :alias "double_id"}}}
              :fields       [$id $name $category_id $latitude $longitude $price *double_id/Float]
              :qp/refs      {$id                                           {:position 0, :alias "ID", :source {:table ::refs/source, :alias "ID"}}
                             $name                                         {:position 1, :alias "NAME", :source {:table ::refs/source, :alias "NAME"}}
                             $category_id                                  {:position 2, :alias "CATEGORY_ID", :source {:table ::refs/source, :alias "CATEGORY_ID"}}
                             $latitude                                     {:position 3, :alias "LATITUDE", :source {:table ::refs/source, :alias "LATITUDE"}}
                             $longitude                                    {:position 4, :alias "LONGITUDE", :source {:table ::refs/source, :alias "LONGITUDE"}}
                             $price                                        {:position 5, :alias "PRICE", :source {:table ::refs/source, :alias "PRICE"}}
                             [:field "double_id" {:base-type :type/Float}] {:position 6, :alias "double_id", :source {:table ::refs/source, :alias "double_id"}}}})
           (add-references
            (mt/mbql-query venues
              {:source-query {:source-table $$venues
                              :expressions  {:double_id [:* $id 2]}
                              :fields       [$id $name $category_id $latitude $longitude $price [:expression "double_id"]]}
               :fields       [$id $name $category_id $latitude $longitude $price *double_id/Float]}))))))

(deftest native-query-refs-test
  (let [native-query (mt/native-query {:query "SELECT * FROM VENUES LIMIT 5;"})
        metadata     (-> (qp/process-query native-query)
                         :data
                         :results_metadata
                         :columns)
        mbql-query   (mt/mbql-query nil
                       {:source-query    {:native "SELECT * FROM VENUES LIMIT 5;"}
                        :source-metadata metadata})]
    (is (query= (-> mbql-query
                    (assoc-in [:query :qp/refs]
                              {[:field "ID" {:base-type :type/BigInteger}]       {:source   {:table ::refs/source
                                                                                             :alias "ID"}
                                                                                  :position 0
                                                                                  :alias    "ID"}
                               [:field "NAME" {:base-type :type/Text}]           {:source   {:table ::refs/source
                                                                                             :alias "NAME"}
                                                                                  :position 1
                                                                                  :alias    "NAME"}
                               [:field "CATEGORY_ID" {:base-type :type/Integer}] {:source   {:table ::refs/source
                                                                                             :alias "CATEGORY_ID"}
                                                                                  :position 2
                                                                                  :alias    "CATEGORY_ID"}
                               [:field "LATITUDE" {:base-type :type/Float}]      {:source   {:table ::refs/source
                                                                                             :alias "LATITUDE"}
                                                                                  :position 3
                                                                                  :alias    "LATITUDE"}
                               [:field "LONGITUDE" {:base-type :type/Float}]     {:source   {:table ::refs/source
                                                                                             :alias "LONGITUDE"}
                                                                                  :position 4
                                                                                  :alias    "LONGITUDE"}
                               [:field "PRICE" {:base-type :type/Integer}]       {:source   {:table ::refs/source
                                                                                             :alias "PRICE"}
                                                                                  :position 5
                                                                                  :alias    "PRICE"}})
                    remove-source-metadata)
                (add-references mbql-query)))))

(deftest expressions-in-source-query-in-join-test
  (is (query= (mt/mbql-query venues
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
                                :fields       [&CategoriesStats.category_id
                                               &CategoriesStats.*MaxPrice/Integer
                                               &CategoriesStats.*AvgPrice/Integer
                                               &CategoriesStats.*MinPrice/Integer]
                                :alias        "CategoriesStats"}]
                 :limit       3
                 :qp/refs     {$id                                 {:alias "ID", :position 0}
                               $name                               {:alias "NAME", :position 1}
                               $category_id                        {:alias "CATEGORY_ID", :position 2}
                               $latitude                           {:alias "LATITUDE", :position 3}
                               $longitude                          {:alias "LONGITUDE", :position 4}
                               $price                              {:alias "PRICE", :position 5}
                               [:expression "RelativePrice"]       {:alias "RelativePrice", :position 6}
                               &CategoriesStats.venues.category_id {:alias "CategoriesStats__CATEGORY_ID", :position 7, :source {:alias "CATEGORY_ID", :table "CategoriesStats"}}
                               &CategoriesStats.*MaxPrice/Integer  {:alias "CategoriesStats__MaxPrice", :position 8, :source {:alias "MaxPrice", :table "CategoriesStats"}}
                               &CategoriesStats.*AvgPrice/Integer  {:alias "CategoriesStats__AvgPrice", :position 9, :source {:alias "AvgPrice", :table "CategoriesStats"}}
                               &CategoriesStats.*MinPrice/Integer  {:alias "CategoriesStats__MinPrice", :position 10, :source {:alias "MinPrice", :table "CategoriesStats"}}}})
              (add-references
               (qp/query->preprocessed
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
                                                 :aggregation  [[:aggregation-options [:max $price] {:name "MaxPrice/"}]
                                                                [:aggregation-options [:avg $price] {:name "AvgPrice"}]
                                                                [:aggregation-options [:min $price] {:name "MinPrice"}]]
                                                 :breakout     [$category_id]}
                                  :alias        "CategoriesStats"
                                  :fields       :all}]
                   :limit       3}))))))

(deftest implicit-join-test
  (is (query= (mt/mbql-query venues
                {:joins    [{:source-table $$categories
                             :alias        "CATEGORIES__via__CATEGORY_ID"
                             :condition    [:= $category_id &CATEGORIES__via__CATEGORY_ID.categories.id]
                             :strategy     :left-join}]
                 :fields   [$name
                            $category_id->&CATEGORIES__via__CATEGORY_ID.categories.name]
                 :order-by [[:asc $id]]
                 :limit    5
                 :qp/refs  {$name                                         {:alias    "NAME"
                                                                           :position 0}
                            &CATEGORIES__via__CATEGORY_ID.categories.name {:alias    "CATEGORIES__via__CATEGORY_ID__NAME"
                                                                           :position 1
                                                                           :source   {:table "CATEGORIES__via__CATEGORY_ID"
                                                                                      :alias "NAME"}}
                            $category_id                                  {:alias "CATEGORY_ID"}
                            $id                                           {:alias "ID"}
                            $latitude                                     {:alias "LATITUDE"}
                            $longitude                                    {:alias "LONGITUDE"}
                            $price                                        {:alias "PRICE"}
                            &CATEGORIES__via__CATEGORY_ID.categories.id   {:alias  "CATEGORIES__via__CATEGORY_ID__ID"
                                                                           :source {:alias "ID"
                                                                                    :table "CATEGORIES__via__CATEGORY_ID"}}}})
              (add-references
               (qp/query->preprocessed
                (mt/mbql-query venues
                  {:joins    [{:source-table $$categories
                               :alias        "CATEGORIES__via__CATEGORY_ID"
                               :condition    [:= $category_id &CATEGORIES__via__CATEGORY_ID.categories.id]
                               :strategy     :left-join}]
                   :fields   [$name
                              $category_id->&CATEGORIES__via__CATEGORY_ID.categories.name]
                   :order-by [[:asc $id]]
                   :limit    5}))))))

(deftest another-source-query-test
  (is (query= (mt/mbql-query checkins
                {:source-query {:source-table $$checkins
                                :breakout     [!month.date]
                                :aggregation  [[:aggregation-options [:sum $user_id] {:name "sum"}]
                                               [:aggregation-options [:sum $venue_id] {:name "sum_2"}]]
                                :order-by     [[:asc !month.date]]
                                :qp/refs      {!month.date      {:alias "DATE", :position 0}
                                               $date            {:alias "DATE"}
                                               $id              {:alias "ID"}
                                               $user_id         {:alias "USER_ID"}
                                               $venue_id        {:alias "VENUE_ID"}
                                               [:aggregation 0] {:alias "sum", :position 1}
                                               [:aggregation 1] {:alias "sum_2", :position 2}}}
                 :fields       [!default.date
                                *sum/Integer
                                *sum_2/Integer]
                 :filter       [:> *sum/Float [:value 300 {:base_type :type/Float}]]
                 :limit        2
                 :qp/refs      {$date                                 {:alias "DATE", :position 0, :source {:alias "DATE", :table ::refs/source}}
                                [:field "sum" {:base-type :type/*}]   {:alias "sum", :position 1, :source {:alias "sum", :table ::refs/source}}
                                *sum/Integer                          {:alias "sum", :source {:alias "sum", :table ::refs/source}}
                                [:field "sum_2" {:base-type :type/*}] {:alias "sum_2", :position 2, :source {:alias "sum_2", :table ::refs/source}}
                                *sum_2/Integer                        {:alias "sum_2", :source {:alias "sum_2", :table ::refs/source}}}})
              (-> (mt/mbql-query checkins
                    {:source-query {:source-table $$checkins
                                    :aggregation  [[:sum $user_id]
                                                   [:sum $venue_id]]
                                    :breakout     [!month.date]}
                     :filter       [:> *sum/Float 300]
                     :limit        2})
                  qp/query->preprocessed
                  add-references))))

(deftest source-query-references-joins-test
  (testing "Make sure `:field` clause with `:join-info` whose join is in the source query has correct `:source` info"
    (is (query= (mt/$ids venues
                  {&Cat.categories.id   {:alias "Cat__ID", :source {:alias "Cat__ID", :table ::refs/source}}
                   &Cat.categories.name {:position 0, :alias "Cat__NAME", :source {:alias "Cat__NAME", :table ::refs/source}}
                   $id                  {:alias "ID", :source {:table ::refs/source, :alias "ID"}}
                   $name                {:alias "NAME", :source {:table ::refs/source, :alias "NAME"}}
                   $category_id         {:alias "CATEGORY_ID", :source {:table ::refs/source, :alias "CATEGORY_ID"}}
                   $latitude            {:alias "LATITUDE", :source {:table ::refs/source, :alias "LATITUDE"}}
                   $longitude           {:alias "LONGITUDE", :source {:table ::refs/source, :alias "LONGITUDE"}}
                   $price               {:alias "PRICE", :source {:table ::refs/source, :alias "PRICE"}}})
                (-> (mt/mbql-query venues
                      {:source-query {:source-table $$venues
                                      :joins        [{:strategy     :left-join
                                                      :source-table $$categories
                                                      :alias        "Cat"
                                                      :condition    [:= $category_id &Cat.categories.id]}]
                                      :fields       [$id]}
                       :breakout     [&Cat.categories.name]})
                    qp/query->preprocessed
                    add-references
                    :query
                    :qp/refs)))))

(deftest source-query-references-joins-test-2
  (testing "`:field` joined against the source query with datetime bucketing should have correct `:source` info"
    (is (query= (mt/$ids venues
                  {&C.checkins.id         {:alias "C__ID", :source {:alias "C__ID", :table ::refs/source}}
                   !year.&C.checkins.date {:position 0, :alias "C__DATE", :source {:alias "C__DATE", :table ::refs/source}}
                   &C.checkins.date       {:alias "C__DATE", :source {:alias "C__DATE", :table ::refs/source}}
                   &C.checkins.venue_id   {:alias "C__VENUE_ID", :source {:alias "C__VENUE_ID", :table ::refs/source}}
                   &C.checkins.user_id    {:alias "C__USER_ID", :source {:alias "C__USER_ID", :table ::refs/source}}
                   $id                    {:alias "ID", :source {:table ::refs/source, :alias "ID"}}
                   $name                  {:alias "NAME", :source {:table ::refs/source, :alias "NAME"}}
                   $category_id           {:alias "CATEGORY_ID", :source {:table ::refs/source, :alias "CATEGORY_ID"}}
                   $latitude              {:alias "LATITUDE", :source {:table ::refs/source, :alias "LATITUDE"}}
                   $longitude             {:alias "LONGITUDE", :source {:table ::refs/source, :alias "LONGITUDE"}}
                   $price                 {:alias "PRICE", :source {:table ::refs/source, :alias "PRICE"}}})
                (-> (mt/mbql-query venues
                      {:source-query {:source-table $$venues
                                      :joins        [{:strategy     :left-join
                                                      :source-table $$checkins
                                                      :alias        "C"
                                                      :condition    [:= $id &C.checkins.venue_id]}]
                                      :fields       [$id]}
                       :breakout     [!year.&C.checkins.date]})
                    qp/query->preprocessed
                    add-references
                    :query
                    :qp/refs)))))

#_(deftest mega-query-refs-test
  (testing "Should generate correct SQL for joins against source queries that contain joins (#12928)"
    (mt/dataset sample-dataset
      (is (= '{:fields
               [&P1.products.category
                &People.people.source
                *count/BigInteger
                &Q2.products.category
                &Q2.*avg/Integer]

               :source-query
               {:source-table $$orders
                :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                :breakout     [&P1.products.category &People.people.source]
                :order-by     [[:asc &P1.products.category] [:asc &People.people.source]]
                :joins        [{:strategy     :left-join
                                :source-table $$products
                                :condition    [:= $product_id &P1.products.id]
                                :alias        "P1"}
                               {:strategy     :left-join
                                :source-table $$people
                                :condition    [:= $user_id &People.people.id]
                                :alias        "People"}]
                :qp/refs      {&P1.products.created_at   {:source {:table "P1", :alias "CREATED_AT"}, :alias "P1__CREATED_AT"}
                               &People.people.birth_date {:source {:table "People", :alias "BIRTH_DATE"}, :alias "People__BIRTH_DATE"}
                               &People.people.created_at {:source {:table "People", :alias "CREATED_AT"}, :alias "People__CREATED_AT"}
                               $created_at               {:alias "CREATED_AT"}
                               $discount                 {:alias "DISCOUNT"}
                               $id                       {:alias "ID"}
                               $product_id               {:alias "PRODUCT_ID"}
                               $quantity                 {:alias "QUANTITY"}
                               $subtotal                 {:alias "SUBTOTAL"}
                               $tax                      {:alias "TAX"}
                               $total                    {:alias "TOTAL"}
                               $user_id                  {:alias "USER_ID"}
                               &P1.products.category     {:position 0, :alias "P1__CATEGORY", :source {:table "P1", :alias "CATEGORY"}}
                               &P1.products.ean          {:source {:table "P1", :alias "EAN"}, :alias "P1__EAN"}
                               &P1.products.id           {:source {:table "P1", :alias "ID"}, :alias "P1__ID"}
                               &P1.products.price        {:source {:table "P1", :alias "PRICE"}, :alias "P1__PRICE"}
                               &P1.products.rating       {:source {:table "P1", :alias "RATING"}, :alias "P1__RATING"}
                               &P1.products.title        {:source {:table "P1", :alias "TITLE"}, :alias "P1__TITLE"}
                               &P1.products.vendor       {:source {:table "P1", :alias "VENDOR"}, :alias "P1__VENDOR"}
                               &People.people.address    {:source {:table "People", :alias "ADDRESS"}, :alias "People__ADDRESS"}
                               &People.people.city       {:source {:table "People", :alias "CITY"}, :alias "People__CITY"}
                               &People.people.email      {:source {:table "People", :alias "EMAIL"}, :alias "People__EMAIL"}
                               &People.people.id         {:source {:table "People", :alias "ID"}, :alias "People__ID"}
                               &People.people.latitude   {:source {:table "People", :alias "LATITUDE"}, :alias "People__LATITUDE"}
                               &People.people.longitude  {:source {:table "People", :alias "LONGITUDE"}, :alias "People__LONGITUDE"}
                               &People.people.name       {:source {:table "People", :alias "NAME"}, :alias "People__NAME"}
                               &People.people.password   {:source {:table "People", :alias "PASSWORD"}, :alias "People__PASSWORD"}
                               &People.people.source     {:position 1, :alias "People__SOURCE", :source {:table "People", :alias "SOURCE"}}
                               &People.people.state      {:source {:table "People", :alias "STATE"}, :alias "People__STATE"}
                               &People.people.zip        {:source {:table "People", :alias "ZIP"}, :alias "People__ZIP"}
                               [:aggregation 0]          {:position 2, :alias "count"}}}

               :joins
               [{:strategy     :left-join
                 :condition    [:= &P1.products.category &Q2.products.category]
                 :alias        "Q2"
                 :source-query {:source-table $$reviews
                                :aggregation  [[:aggregation-options [:avg $reviews.rating] {:name "avg"}]]
                                :breakout     [&P2.products.category]
                                :joins        [{:strategy     :left-join
                                                :source-table $$products
                                                :condition    [:= $reviews.product_id &P2.products.id]
                                                :alias        "P2"}]
                                :order-by     [[:asc &P2.products.category]]
                                :qp/refs      {&P2.products.vendor     {:source {:table "P2", :alias "VENDOR"}, :alias "P2__VENDOR"}
                                               &P2.products.id         {:source {:table "P2", :alias "ID"}, :alias "P2__ID"}
                                               &P2.products.ean        {:source {:table "P2", :alias "EAN"}, :alias "P2__EAN"}
                                               &P2.products.category   {:position 0, :alias "P2__CATEGORY", :source {:table "P2", :alias "CATEGORY"}}
                                               $reviews.rating         {:alias "RATING"}
                                               $reviews.body           {:alias "BODY"}
                                               $reviews.product_id     {:alias "PRODUCT_ID"}
                                               $reviews.id             {:alias "ID"}
                                               $reviews.created_at     {:alias "CREATED_AT"}
                                               &P2.products.price      {:source {:table "P2", :alias "PRICE"}, :alias "P2__PRICE"}
                                               $reviews.reviewer       {:alias "REVIEWER"}
                                               &P2.products.created_at {:source {:table "P2", :alias "CREATED_AT"}, :alias "P2__CREATED_AT"}
                                               &P2.products.title      {:source {:table "P2", :alias "TITLE"}, :alias "P2__TITLE"}
                                               &P2.products.rating     {:source {:table "P2", :alias "RATING"}, :alias "P2__RATING"}
                                               [:aggregation 0]        {:alias "avg", :position 1}}}}]
               :limit   2
               :qp/refs {&P1.products.category {:position 0, :alias "P1__CATEGORY", :source {:table ::refs/source, :alias "P1__CATEGORY"}}
                         &People.people.source {:position 1, :alias "People__SOURCE", :source {:table ::refs/source, :alias "People__SOURCE"}}
                         *count/BigInteger     {:position 2, :alias "count", :source {:alias "count", :table ::refs/source}}
                         &Q2.products.category {:position 3, :alias "Q2__P2__CATEGORY", :source {:table "Q2", :alias "P2__CATEGORY"}}
                         &Q2.*avg/Integer      {:position 4, :alias "Q2__avg", :source {:table "Q2", :alias "avg"}}}}
             (-> (mt/mbql-query nil
                   {:fields       [&P1.products.category
                                   &People.people.source
                                   [:field "count" {:base-type :type/BigInteger}]
                                   &Q2.products.category
                                   [:field "avg" {:base-type :type/Integer, :join-alias "Q2"}]]
                    :source-query {:source-table $$orders
                                   :aggregation  [[:aggregation-options [:count] {:name "count"}]]
                                   :breakout     [&P1.products.category
                                                  &People.people.source]
                                   :order-by     [[:asc &P1.products.category]
                                                  [:asc &People.people.source]]
                                   :joins        [{:strategy     :left-join
                                                   :source-table $$products
                                                   :condition    [:= $orders.product_id &P1.products.id]
                                                   :alias        "P1"}
                                                  {:strategy     :left-join
                                                   :source-table $$people
                                                   :condition    [:= $orders.user_id &People.people.id]
                                                   :alias        "People"}]}
                    :joins        [{:strategy     :left-join
                                    :condition    [:= &P1.products.category &Q2.products.category]
                                    :alias        "Q2"
                                    :source-query {:source-table $$reviews
                                                   :aggregation  [[:aggregation-options [:avg $reviews.rating] {:name "avg"}]]
                                                   :breakout     [&P2.products.category]
                                                   :joins        [{:strategy     :left-join
                                                                   :source-table $$products
                                                                   :condition    [:= $reviews.product_id &P2.products.id]
                                                                   :alias        "P2"}]}}]
                    :limit        2})
                 qp/query->preprocessed
                 add-references
                 (m/dissoc-in [:query :source-metadata])
                 (m/dissoc-in [:query :joins 0 :source-metadata])
                 debug-qp/to-mbql-shorthand
                 last))))))

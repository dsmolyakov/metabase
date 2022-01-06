(ns metabase.driver.sql.query-processor-test
  (:require [clojure.test :refer :all]
            [honeysql.core :as hsql]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.test-util :as sql-jdbc.tu]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.query-processor-test-util :as sql.qp-test-util]
            [metabase.models.field :refer [Field]]
            [metabase.models.setting :as setting]
            [metabase.query-processor :as qp]
            [metabase.query-processor.interface :as qp.i]
            [metabase.test :as mt]
            [metabase.util.honeysql-extensions :as hx]
            [schema.core :as s]))

(deftest field-ref-info-test
  (testing "field-ref-info should ignore namespaced options added by various driver QP methods"
    (binding [sql.qp/*query* {:qp/refs {[:field 1 nil] {:alias "wow"}}}]
      (is (= {:alias "wow"}
             (#'sql.qp/field-ref-info [:field 1 {::crazy-option 1000}]))))))

(deftest process-mbql-query-keys-test
  (testing "make sure our logic for deciding which order to process keys in the query works as expected"
    (is (= [:source-table :breakout :aggregation :fields :abc :def]
           (#'sql.qp/query->keys-in-application-order
            {:def          6
             :abc          5
             :source-table 1
             :aggregation  3
             :fields       4
             :breakout     2})))))

(defn- mbql->native [query]
  (mt/with-everything-store
    (driver/with-driver :h2
      (-> (sql.qp/mbql->native :h2 (qp/query->preprocessed query))
          :query
          sql.qp-test-util/pretty-sql))))

(deftest join-test
  (testing "Test that correct identifiers are used for joins"
    (mt/with-everything-store
      (is (= '{:select    [VENUES.ID AS ID
                           VENUES.NAME AS NAME
                           VENUES.CATEGORY_ID AS CATEGORY_ID
                           VENUES.LATITUDE AS LATITUDE
                           VENUES.LONGITUDE AS LONGITUDE
                           VENUES.PRICE AS PRICE]
               :from      [VENUES]
               :left-join [CATEGORIES c
                           ON VENUES.CATEGORY_ID = c.ID]
               :where     [c.NAME = ?]
               :order-by  [VENUES.ID ASC]
               :limit     [100]}
             (-> (mt/mbql-query venues
                   {:source-table $$venues
                    :order-by     [[:asc $id]]
                    :filter       [:=
                                   &c.categories.name
                                   [:value "BBQ" {:base_type :type/Text, :semantic_type :type/Name, :database_type "VARCHAR"}]]
                    :fields       [$id $name $category_id $latitude $longitude $price]
                    :limit        100
                    :joins        [{:source-table $$categories
                                    :alias        "c"
                                    :strategy     :left-join
                                    :condition    [:=
                                                   $category_id
                                                   &c.categories.id]
                                    :fk-field-id  (mt/id :venues :category_id)
                                    :fields       :none}]})
                 mbql->native
                 sql.qp-test-util/sql->sql-map))))))

(deftest nested-query-and-join-test
  (testing "This HAIRY query tests that the correct identifiers and aliases are used with both a nested query and JOIN in play."
    (mt/with-everything-store
      (driver/with-driver :h2
        (is (= '{:select    [v.NAME AS v__NAME
                             count (*) AS count]
                 :from      [{:select [CHECKINS.ID AS ID
                                       CHECKINS.DATE AS DATE
                                       CHECKINS.USER_ID AS USER_ID
                                       CHECKINS.VENUE_ID AS VENUE_ID]
                              :from   [CHECKINS],
                              :where  [CHECKINS.DATE > ?]}
                             source],
                 :left-join [VENUES v
                             ON source.VENUE_ID = v.ID],
                 :where     [((v.NAME like ?) AND source.USER_ID > 0)]
                 :group-by  [v.NAME]
                 :order-by  [v.NAME ASC]}
               (-> (mt/mbql-query checkins
                     {:source-query {:source-table $$checkins
                                     :fields       [$id [:field %date {:temporal-unit :default}] $user_id $venue_id]
                                     :filter       [:>
                                                    $date
                                                    [:absolute-datetime #t "2015-01-01T00:00:00.000000000-00:00" :default]]}
                      :aggregation  [[:count]]
                      :order-by     [[:asc &v.venues.name]]
                      :breakout     [&v.venues.name]
                      :filter       [:and
                                     [:starts-with
                                      &v.venues.name
                                      [:value "F" {:base_type     :type/Text
                                                   :semantic_type :type/Name
                                                   :database_type "VARCHAR"}]]
                                     [:> [:field "USER_ID" {:base-type :type/Integer}] 0]]
                      :joins        [{:source-table $$venues
                                      :alias        "v"
                                      :strategy     :left-join
                                      :condition    [:=
                                                     $venue_id
                                                     &v.venues.id]
                                      :fk-field-id  (mt/id :checkins :venue_id)
                                      :fields       :none}]})
                   mbql->native
                   sql.qp-test-util/sql->sql-map)))))))

(deftest handle-named-aggregations-test
  (testing "Check that named aggregations are handled correctly"
    (mt/with-everything-store
      (driver/with-driver :h2
        (is (= '{:select   [VENUES.PRICE AS PRICE
                            avg (VENUES.CATEGORY_ID) AS avg_2]
                 :from     [VENUES]
                 :group-by [VENUES.PRICE]
                 :order-by [avg_2 ASC
                            VENUES.PRICE ASC]}
               (-> (mt/mbql-query venues
                     {:aggregation [[:aggregation-options [:avg $category_id] {:name "avg_2"}]]
                      :breakout    [$price]
                      :order-by    [[:asc [:aggregation 0]]]})
                   mbql->native
                   sql.qp-test-util/sql->sql-map)))))))

(deftest handle-source-query-params-test
  (testing "params from source queries should get passed in to the top-level. Semicolons should be removed"
    (mt/with-everything-store
      (driver/with-driver :h2
        (is (= {:query  "SELECT \"source\".* FROM (SELECT * FROM some_table WHERE name = ?) \"source\" WHERE (\"source\".\"name\" <> ? OR \"source\".\"name\" IS NULL)"
                :params ["Cam" "Lucky Pigeon"]}
               (sql.qp/mbql->native
                :h2
                (mt/mbql-query venues
                  {:source-query {:native "SELECT * FROM some_table WHERE name = ?;", :params ["Cam"]}
                   :source-metadata [{:name "name", :base_type :type/Integer}]
                   :filter       [:!= *name/Integer "Lucky Pigeon"]}))))))))

(deftest joins-against-native-queries-test
  (testing "Joins against native SQL queries should get converted appropriately! make sure correct HoneySQL is generated"
    (mt/with-everything-store
      (driver/with-driver :h2
        (is (= [[(sql.qp/->SQLSourceQuery "SELECT * FROM VENUES;" [])
                 (hx/identifier :table-alias "card")]
                [:=
                 (hx/with-database-type-info (hx/identifier :field "PUBLIC" "CHECKINS" "VENUE_ID") "integer")
                 (hx/identifier :field "id")]]
               (sql.qp/join->honeysql :h2
                                      (mt/$ids checkins
                                               {:source-query {:native "SELECT * FROM VENUES;", :params []}
                                                :alias        "card"
                                                :strategy     :left-join
                                                :condition    [:= $venue_id &card.*id/Integer]}))))))))

(deftest compile-honeysql-test
  (testing "make sure the generated HoneySQL will compile to the correct SQL"
    (is (= ["INNER JOIN (SELECT * FROM VENUES) card ON PUBLIC.CHECKINS.VENUE_ID = id"]
           (hsql/format {:join (mt/with-everything-store
                                 (driver/with-driver :h2
                                   (sql.qp/join->honeysql :h2
                                     (mt/$ids checkins
                                       {:source-query {:native "SELECT * FROM VENUES;", :params []}
                                        :alias        "card"
                                        :strategy     :left-join
                                        :condition    [:= $venue_id &card.*id/Integer]}))))})))))

(deftest adjust-start-of-week-test
  (driver/with-driver :h2
    (with-redefs [driver/db-start-of-week   (constantly :monday)
                  setting/get-value-of-type (constantly :sunday)]
      (is (= (hsql/call :dateadd
               (hx/literal "day")
               (hx/with-database-type-info (hsql/call :cast -1 #sql/raw "long") "long")
               (hsql/call :week (hsql/call :dateadd (hx/literal "day")
                                  (hx/with-database-type-info (hsql/call :cast 1 #sql/raw "long") "long")
                                  :created_at)))
             (sql.qp/adjust-start-of-week :h2 (partial hsql/call :week) :created_at))))
    (testing "Do we skip the adjustment if offset = 0"
      (with-redefs [driver/db-start-of-week   (constantly :monday)
                    setting/get-value-of-type (constantly :monday)]
        (is (= (hsql/call :week :created_at)
               (sql.qp/adjust-start-of-week :h2 (partial hsql/call :week) :created_at)))))))

(defn- query-on-dataset-with-nils
  [query]
  (mt/rows
    (qp/process-query
     {:database (mt/id)
      :type     :query
      :query    (merge
                 {:source-query {:native "select 'foo' as a union select null as a union select 'bar' as a"}
                  :order-by     [[:asc [:field "A" {:base-type :type/Text}]]]}
                 query)})))

(deftest correct-for-null-behaviour
  (testing "NULLs should be treated intuitively in filters (SQL has somewhat unintuitive semantics where NULLs get propagated out of expressions)."
    (is (= [[nil] ["bar"]]
           (query-on-dataset-with-nils {:filter [:not [:starts-with [:field "A" {:base-type :type/Text}] "f"]]})))
    (is (= [[nil] ["bar"]]
           (query-on-dataset-with-nils {:filter [:not [:ends-with [:field "A" {:base-type :type/Text}] "o"]]})))
    (is (= [[nil] ["bar"]]
           (query-on-dataset-with-nils {:filter [:not [:contains [:field "A" {:base-type :type/Text}] "f"]]})))
    (is (= [[nil] ["bar"]]
           (query-on-dataset-with-nils {:filter [:!= [:field "A" {:base-type :type/Text}] "foo"]}))))
  (testing "Null behaviour correction fix should work with joined fields (#13534)"
    (is (= [[1000]]
           (mt/rows
             (mt/run-mbql-query checkins
               {:filter      [:!= &u.users.name "foo"]
                :aggregation [:count]
                :joins       [{:source-table $$users
                               :alias        "u"
                               :condition    [:= $user_id &u.users.id]}]}))))))

(deftest joined-field-clauses-test
  (testing "Should correctly compile `:field` clauses with `:join-alias`"
    (testing "when the join is at the same level"
      (is (= {:select    '[c.NAME AS c__NAME]
              :from      '[VENUES]
              :left-join '[CATEGORIES c ON VENUES.CATEGORY_ID = c.ID]
              :limit     [qp.i/absolute-max-results]}
             (-> (mt/mbql-query venues
                   {:fields [&c.categories.name]
                    :joins  [{:fields       [&c.categories.name]
                              :source-table $$categories
                              :strategy     :left-join
                              :condition    [:= $category_id &c.categories.id]
                              :alias        "c"}]})
                 mbql->native
                 sql.qp-test-util/sql->sql-map))))

    (testing "when the join is NOT at the same level"
      (is (= {:select '[source.c__NAME AS c__NAME]
              :from   '[{:select    [c.NAME AS c__NAME]
                         :from      [VENUES]
                         :left-join [CATEGORIES c ON VENUES.CATEGORY_ID = c.ID]} source]
              :limit  [qp.i/absolute-max-results]}
             (-> (mt/mbql-query venues
                   {:fields       [&c.categories.name]
                    :source-query {:source-table $$venues
                                   :fields       [&c.categories.name]
                                   :joins        [{:fields       [&c.categories.name]
                                                   :source-table $$categories
                                                   :strategy     :left-join
                                                   :condition    [:= $category_id &c.categories.id]
                                                   :alias        "c"}]}})
                 mbql->native
                 sql.qp-test-util/sql->sql-map))))))

(deftest ambiguous-field-metadata-test
  (testing "With queries that refer to the same field more than once, can we generate sane SQL?"
    (mt/dataset sample-dataset
      (is (= '{:select    [ORDERS.ID AS ID
                           ORDERS.PRODUCT_ID AS PRODUCT_ID
                           PRODUCTS__via__PRODUCT_ID.TITLE AS PRODUCTS__via__PRODUCT_ID__TITLE
                           Products.ID AS Products__ID
                           Products.TITLE AS Products__TITLE]
               :from      [ORDERS]
               :left-join [PRODUCTS Products
                           ON ORDERS.PRODUCT_ID = Products.ID
                           PRODUCTS PRODUCTS__via__PRODUCT_ID
                           ON ORDERS.PRODUCT_ID = PRODUCTS__via__PRODUCT_ID.ID]
               :order-by  [ORDERS.ID ASC]
               :limit     [2]}
             (-> (mt/mbql-query orders
                   {:joins    [{:strategy     :left-join
                                :source-table $$products
                                :condition    [:= $product_id &Products.products.id]
                                :alias        "Products"}
                               {:strategy     :left-join
                                :source-table $$products
                                :alias        "PRODUCTS__via__PRODUCT_ID"
                                :fk-field-id  %product_id
                                :condition    [:= $product_id &PRODUCTS__via__PRODUCT_ID.products.id]}]
                    :order-by [[:asc $id]]
                    :limit    2
                    :fields   [$id
                               $product_id
                               &PRODUCTS__via__PRODUCT_ID.products.title
                               &Products.products.id
                               &Products.products.title]})
                 mbql->native
                 sql.qp-test-util/sql->sql-map))))))

(deftest simple-expressions-test
  (is (= '{:select [source.id AS id
                    source.name AS name
                    source.category_id AS category_id
                    source.latitude AS latitude
                    source.longitude AS longitude
                    source.price AS price
                    source.double_id AS double_id]
           :from   [{:select [public.venues.id AS id
                              public.venues.name AS name
                              public.venues.category_id AS category_id
                              public.venues.latitude AS latitude
                              public.venues.longitude AS longitude
                              public.venues.price AS price
                              (public.venues.id * 2) AS double_id]
                     :from   [public.venues]}
                    source]
           :limit  [1]}
         (-> (mt/mbql-query venues
               {:source-query {:source-table $$venues
                               :expressions  {:double_id [:* $id 2]}
                               :fields       [$id $name $category_id $latitude $longitude $price [:expression "double_id"]]}
                :fields       [$id $name $category_id $latitude $longitude $price *double_id/Float]
                :limit        1})
             mbql->native
             sql.qp-test-util/sql->sql-map))))

(deftest multiple-joins-with-expressions-test
  (testing "We should be able to compile a complicated query with multiple joins and expressions correctly"
    (mt/dataset sample-dataset
      (is (= (str "SELECT source.PRODUCTS__via__PRODUCT_ID__CATEGORY AS PRODUCTS__via__PRODUCT_ID__CATEGORY,"
                  " source.PEOPLE__via__USER_ID__SOURCE AS PEOPLE__via__USER_ID__SOURCE,"
                  " parsedatetime(year(source.CREATED_AT), 'yyyy') AS CREATED_AT,"
                  " source.\"pivot-grouping\" AS \"pivot-grouping\", count(*) AS count "
                  "FROM ("
                  "SELECT PRODUCTS__via__PRODUCT_ID.CATEGORY AS PRODUCTS__via__PRODUCT_ID__CATEGORY,"
                  " PEOPLE__via__USER_ID.SOURCE AS PEOPLE__via__USER_ID__SOURCE,"
                  " ORDERS.CREATED_AT AS CREATED_AT,"
                  " abs(0) AS \"pivot-grouping\" "
                  "FROM ORDERS"
                  " LEFT JOIN PRODUCTS PRODUCTS__via__PRODUCT_ID"
                  " ON ORDERS.PRODUCT_ID = PRODUCTS__via__PRODUCT_ID.ID "
                  "LEFT JOIN PEOPLE PEOPLE__via__USER_ID"
                  " ON ORDERS.USER_ID = PEOPLE__via__USER_ID.ID"
                  ") source "
                  "WHERE ((source.PEOPLE__via__USER_ID__SOURCE = ? OR source.PEOPLE__via__USER_ID__SOURCE = ?)"
                  " AND (source.PRODUCTS__via__PRODUCT_ID__CATEGORY = ?"
                  " OR source.PRODUCTS__via__PRODUCT_ID__CATEGORY = ?)"
                  " AND source.CREATED_AT >= parsedatetime(year(dateadd('year', CAST(-2 AS long), now())), 'yyyy')"
                  " AND source.CREATED_AT < parsedatetime(year(now()), 'yyyy')) "
                  "GROUP BY source.PRODUCTS__via__PRODUCT_ID__CATEGORY,"
                  " source.PEOPLE__via__USER_ID__SOURCE,"
                  " parsedatetime(year(source.CREATED_AT), 'yyyy'),"
                  " source.\"pivot-grouping\" "
                  "ORDER BY source.PRODUCTS__via__PRODUCT_ID__CATEGORY ASC,"
                  " source.PEOPLE__via__USER_ID__SOURCE ASC,"
                  " parsedatetime(year(source.CREATED_AT), 'yyyy') ASC,"
                  " source.\"pivot-grouping\" ASC")
             (mbql->native
              (mt/mbql-query orders
                {:aggregation [[:aggregation-options [:count] {:name "count"}]]
                 :breakout    [&PRODUCTS__via__PRODUCT_ID.products.category
                               &PEOPLE__via__USER_ID.people.source
                               !year.created_at
                               [:expression "pivot-grouping"]]
                 :filter     [:and
                              [:or
                               [:=
                                &PEOPLE__via__USER_ID.people.source
                                [:value "Facebook" {:base_type :type/Text, :semantic_type nil, :database_type "VARCHAR", :name "SOURCE"}]]
                               [:=
                                &PEOPLE__via__USER_ID.people.source
                                [:value "Google" {:base_type :type/Text, :semantic_type nil, :database_type "VARCHAR", :name "SOURCE"}]]]
                              [:or
                               [:=
                                &PRODUCTS__via__PRODUCT_ID.products.category
                                [:value "Doohickey" {:base_type :type/Text, :semantic_type nil, :database_type "VARCHAR", :name "CATEGORY"}]]
                               [:=
                                &PRODUCTS__via__PRODUCT_ID.products.category
                                [:value "Gizmo" {:base_type :type/Text, :semantic_type nil, :database_type "VARCHAR", :name "CATEGORY"}]]]
                              [:between !year.created_at [:relative-datetime -2 :year] [:relative-datetime -1 :year]]]
                 :expressions {:pivot-grouping [:abs 0]}
                 :order-by    [[:asc &PRODUCTS__via__PRODUCT_ID.products.category]
                               [:asc &PEOPLE__via__USER_ID.people.source]
                               [:asc !year.created_at]
                               [:asc [:expression "pivot-grouping"]]]
                 :joins       [{:source-table $$products
                                :strategy     :left-join
                                :alias        "PRODUCTS__via__PRODUCT_ID"
                                :fk-field-id  %product_id
                                :condition    [:= $product_id &PRODUCTS__via__PRODUCT_ID.products.id]}
                               {:source-table $$people
                                :strategy     :left-join
                                :alias        "PEOPLE__via__USER_ID"
                                :fk-field-id  %user_id
                                :condition    [:= $user_id &PEOPLE__via__USER_ID.people.id]}]})))))))

(deftest reference-aggregation-expressions-in-joins-test
  (testing "See if we can correctly compile a query that references expressions that come from a join"
    (is (= '{:select [source.ID AS ID
                      source.NAME AS NAME
                      source.CATEGORY_ID AS CATEGORY_ID
                      source.LATITUDE AS LATITUDE
                      source.LONGITUDE AS LONGITUDE
                      source.PRICE AS PRICE
                      source.RelativePrice AS RelativePrice
                      source.CategoriesStats__CATEGORY_ID AS CategoriesStats__CATEGORY_ID
                      source.MaxPrice AS CategoriesStats__MaxPrice
                      source.AvgPrice AS CategoriesStats__AvgPrice
                      source.MinPrice AS CategoriesStats__MinPrice]
             :from   [{:select    [VENUES.ID AS ID
                                   VENUES.NAME AS NAME
                                   VENUES.CATEGORY_ID AS CATEGORY_ID
                                   VENUES.LATITUDE AS LATITUDE
                                   VENUES.LONGITUDE AS LONGITUDE
                                   VENUES.PRICE AS PRICE
                                   (CAST (VENUES.PRICE AS float)
                                         /
                                         CASE WHEN CategoriesStats.AvgPrice = 0 THEN NULL
                                         ELSE CategoriesStats.AvgPrice END) AS RelativePrice
                                   CategoriesStats.CATEGORY_ID AS CategoriesStats__CATEGORY_ID
                                   CategoriesStats.MaxPrice AS CategoriesStats__MaxPrice
                                   CategoriesStats.AvgPrice AS CategoriesStats__AvgPrice
                                   CategoriesStats.MinPrice AS CategoriesStats__MinPrice]
                       :from      [VENUES]
                       :left-join [{:select   [VENUES.CATEGORY_ID AS CATEGORY_ID
                                               max (VENUES.PRICE) AS MaxPrice
                                               avg (VENUES.PRICE) AS AvgPrice
                                               min (VENUES.PRICE) AS MinPrice]
                                    :from     [VENUES]
                                    :group-by [VENUES.CATEGORY_ID]
                                    :order-by [VENUES.CATEGORY_ID ASC]} CategoriesStats
                                   ON VENUES.CATEGORY_ID = CategoriesStats.CATEGORY_ID]}
                      source]
             :limit  [3]}
           (-> (mt/mbql-query venues
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
                                 :condition    [:= $category_id &CategoriesStats.venues.category_id]
                                 :source-query {:source-table $$venues
                                                :aggregation  [[:aggregation-options [:max $price] {:name "MaxPrice"}]
                                                               [:aggregation-options [:avg $price] {:name "AvgPrice"}]
                                                               [:aggregation-options [:min $price] {:name "MinPrice"}]]
                                                :breakout     [$category_id]}
                                 :alias        "CategoriesStats"
                                 :fields       :all}]
                  :limit       3})
               mbql->native
               sql.qp-test-util/sql->sql-map)))))

(deftest expressions-and-coercions-test
  (mt/test-drivers (conj (sql-jdbc.tu/sql-jdbc-drivers) :bigquery)
    (testing "Don't cast in both inner select and outer select when expression (#12430)"
      (let [price-field-id (mt/id :venues :price)]
        (mt/with-temp-vals-in-db Field price-field-id {:coercion_strategy :Coercion/UNIXSeconds->DateTime
                                                       :effective_type    :type/DateTime}
          (let [results (qp/process-query {:database   (mt/id)
                                           :query      {:source-table (mt/id :venues)
                                                        :expressions  {:test ["*" 1 1]}
                                                        :fields       [[:field price-field-id nil]
                                                                       [:expression "test"]]}
                                           :type       "query"})]
            (is (schema= [(s/one s/Str "date")
                          (s/one s/Num "expression")]
                         (-> results mt/rows first)))))))))

(deftest nested-mbql-source-query-test
  (is (= '{:select    [VENUES.ID AS ID
                       VENUES.NAME AS NAME
                       VENUES.CATEGORY_ID AS CATEGORY_ID
                       VENUES.LATITUDE AS LATITUDE
                       VENUES.LONGITUDE AS LONGITUDE
                       VENUES.PRICE AS PRICE]
           :from      [VENUES]
           :left-join [{:select [CATEGORIES.ID AS ID
                                 CATEGORIES.NAME AS NAME]
                        :from   [CATEGORIES]} cat
                       ON VENUES.CATEGORY_ID = cat.ID]
           :order-by  [VENUES.NAME ASC]
           :limit     [3]}
         (-> (mt/mbql-query venues
               {:source-table $$venues
                :joins        [{:alias        "cat"
                                :source-query {:source-table $$categories}
                                :condition    [:= $category_id &cat.*categories.id]}]
                :order-by     [[:asc $name]]
                :limit        3})
             mbql->native
             sql.qp-test-util/sql->sql-map))))

(deftest join-inside-source-query-test
  (testing "Make sure a JOIN inside a source query gets compiled as expected"
    (mt/dataset sample-dataset
      (mt/with-everything-store
        (is (= '{:select [source.P1__CATEGORY AS P1__CATEGORY]
                 :from   [{:select    [P1.CATEGORY AS P1__CATEGORY]
                           :from      [ORDERS]
                           :left-join [PRODUCTS P1 ON ORDERS.PRODUCT_ID = P1.ID]}
                          source]
                 :limit  [1]}
               (-> (mt/mbql-query orders
                     {:fields       [&P1.products.category]
                      :source-query {:source-table $$orders
                                     :fields       [&P1.products.category]
                                     :joins        [{:strategy     :left-join
                                                     :source-table $$products
                                                     :condition    [:= $product_id &P1.products.id]
                                                     :alias        "P1"}]}
                      :limit        1})
                   mbql->native
                   sql.qp-test-util/sql->sql-map)))))))

(deftest join-against-source-query-test
  (testing "Make sure a JOIN referencing fields from the source query use correct aliases/etc"
    (mt/dataset sample-dataset
      (mt/with-everything-store
        (is (= '{:select    [source.P1__CATEGORY AS P1__CATEGORY]
                 :from      [{:select    [P1.CATEGORY AS P1__CATEGORY]
                              :from      [ORDERS]
                              :left-join [PRODUCTS P1 ON ORDERS.PRODUCT_ID = P1.ID]}
                             source]
                 :left-join [{:select    [P2.CATEGORY AS P2__CATEGORY]
                              :from      [REVIEWS]
                              :left-join [PRODUCTS P2 ON REVIEWS.PRODUCT_ID = P2.ID]}
                             Q2
                             ON source.P1__CATEGORY = Q2.P2__CATEGORY]
                 :limit     [1]}
               (-> (mt/mbql-query orders
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
                      :limit        1})
                   mbql->native
                   sql.qp-test-util/sql->sql-map)))))))

(deftest mega-query-test
  (testing "Should generate correct SQL for joins against source queries that contain joins (#12928)"
    (mt/dataset sample-dataset
      (is (= '{:select    [source.P1__CATEGORY AS P1__CATEGORY
                           source.People__SOURCE AS People__SOURCE
                           source.count AS count
                           Q2.P2__CATEGORY AS Q2__P2__CATEGORY
                           Q2.avg AS Q2__avg]
               :from      [{:select    [P1.CATEGORY AS P1__CATEGORY
                                        People.SOURCE AS People__SOURCE
                                        count (*) AS count]
                            :from      [ORDERS]
                            :left-join [PRODUCTS P1 ON ORDERS.PRODUCT_ID = P1.ID
                                        PEOPLE People ON ORDERS.USER_ID = People.ID]
                            :group-by  [P1.CATEGORY People.SOURCE]
                            :order-by  [P1.CATEGORY ASC People.SOURCE ASC]}
                           source]
               :left-join [{:select    [P2.CATEGORY AS P2__CATEGORY avg (REVIEWS.RATING) AS avg]
                            :from      [REVIEWS]
                            :left-join [PRODUCTS P2
                                        ON REVIEWS.PRODUCT_ID = P2.ID]
                            :group-by  [P2.CATEGORY]}
                           Q2 ON source.P1__CATEGORY = Q2.P2__CATEGORY]
               :limit     [2]}
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
                                    :condition    [:= $products.category &Q2.products.category]
                                    :alias        "Q2"
                                    :source-query {:source-table $$reviews
                                                   :aggregation  [[:aggregation-options [:avg $reviews.rating] {:name "avg"}]]
                                                   :breakout     [&P2.products.category]
                                                   :joins        [{:strategy     :left-join
                                                                   :source-table $$products
                                                                   :condition    [:= $reviews.product_id &P2.products.id]
                                                                   :alias        "P2"}]}}]
                    :limit        2})
                 mbql->native
                 sql.qp-test-util/sql->sql-map))))))

from tarea2.sales import get_cash_register_with_most_sales, \
                         get_cash_register_with_least_sales, \
                         get_sales_percentile, \
                         get_product_with_more_units_sold, \
                         get_product_with_more_sales

def get_schema():
    return ['numero_caja', 'producto', 'cantidad', 'precio_unitario', 'venta']

#############################################################################################
######################################## HAPPY PATHS ########################################
#############################################################################################

def test_get_cash_register_with_most_sales_happy_path(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 2
    actual = get_cash_register_with_most_sales(sales_df)

    assert expected == actual


def test_get_cash_register_with_least_sales_happy_path(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 1
    actual = get_cash_register_with_least_sales(sales_df)

    assert expected == actual


def test_get_sales_percentile_happy_path(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 65.0
    actual = get_sales_percentile(sales_df, 0.25)

    assert expected == actual

def test_get_product_with_more_units_sold_happy_path(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 'mango'
    actual = get_product_with_more_units_sold(sales_df)

    assert expected == actual

def test_get_product_with_more_sales_happy_path(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 'mango'
    actual = get_product_with_more_sales(sales_df)

    assert expected == actual

#############################################################################################
####################### get_cash_register_with_most_sales TESTS #############################
#############################################################################################

def test_get_cash_register_with_most_sales_only_one_sale(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 1
    actual = get_cash_register_with_most_sales(sales_df)

    assert expected == actual

def test_get_cash_register_with_most_sales_sales_null():
    sales_df = None
    actual = get_cash_register_with_most_sales(sales_df)

    assert actual is None

def test_get_cash_register_with_most_sales_similars_but_not_same_sales(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (2, 'manzana', 2, 26, 52)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 2
    actual = get_cash_register_with_most_sales(sales_df)

    assert expected == actual

#############################################################################################
####################### get_cash_register_with_least_sales TESTS ############################
#############################################################################################

def test_get_cash_register_with_least_sales_only_one_sale(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 1
    actual = get_cash_register_with_least_sales(sales_df)

    assert expected == actual

def test_get_cash_register_with_least_sales_sales_null():
    sales_df = None
    actual = get_cash_register_with_least_sales(sales_df)

    assert actual is None

def test_get_cash_register_with_least_sales_similars_but_not_same_sales(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (2, 'manzana', 2, 26, 52)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 1
    actual = get_cash_register_with_least_sales(sales_df)

    assert expected == actual

#############################################################################################
########################### get_sales_percentile TESTS ######################################
#############################################################################################

def test_get_sales_percentile_when_25_percentile(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 65.0
    actual = get_sales_percentile(sales_df, 0.25)

    assert expected == actual

def test_get_sales_percentile_when_50_percentile(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 70.0
    actual = get_sales_percentile(sales_df, 0.50)

    assert expected == actual

def test_get_sales_percentile_when_100_percentile(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 80.0
    actual = get_sales_percentile(sales_df, 1)

    assert expected == actual

def test_get_sales_percentile_when_75_percentile(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 75.0
    actual = get_sales_percentile(sales_df, 0.75)

    assert expected == actual

#############################################################################################
####################### get_product_with_more_units_sold TESTS ##############################
#############################################################################################

def test_get_product_with_more_units_sold_when_less_sales_but_more_units(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 4, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 'manzana'
    actual = get_product_with_more_units_sold(sales_df)

    assert expected == actual

def test_get_product_with_more_units_sold_when_only_one_sale(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 'mango'
    actual = get_product_with_more_units_sold(sales_df)

    assert expected == actual

def test_get_product_with_more_units_sold_when_dataset_null():
    sales_df = None
    actual = get_product_with_more_units_sold(sales_df)

    assert actual is None

#############################################################################################
############################ get_product_with_more_sales TESTS ##############################
#############################################################################################

def test_get_product_with_more_sales_sold_when_less_sales_but_more_amount(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 15, 10, 150), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 'manzana'
    actual = get_product_with_more_sales(sales_df)

    assert expected == actual

def test_get_product_with_more_sales_sold_when_only_one_sale(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = 'mango'
    actual = get_product_with_more_sales(sales_df)

    assert expected == actual

def test_get_product_with_more_sales_sold_when_dataset_null():
    sales_df = None
    actual = get_product_with_more_sales(sales_df)

    assert actual is None
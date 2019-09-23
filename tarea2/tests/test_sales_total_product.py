from tarea2.sales import get_total_by_products


def get_schema():
        return ['numero_caja', 'producto', 'cantidad', 'precio_unitario', 'venta']

def test_get_total_by_products_happy_path(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 2, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130),('manzana', 10)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_products_when_data_set_is_null():
    sales_df = None
    actual = get_total_by_products(sales_df)

    assert actual is None

def test_get_total_by_products_when_only_one_sale(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 50)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_only_one_product(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'mango', 3, 50, 150), (1, 'mango', 3, 50, 150)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 350)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_quantity_is_zero_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 0, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_quantity_is_negative_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', -1, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_cr_is_negative_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (-1, 'manzana', 1, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_cr_is_zero_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (0, 'manzana', 1, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_product_is_null_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, None, 1, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_product_is_blank_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, '', 1, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_sale_is_negative_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, -10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_sale_is_zero_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, 0), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


def test_get_total_by_products_when_sale_is_none_should_ignore(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 1, 10, None), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([('mango', 130)], ['producto', 'ventas'])
    actual = get_total_by_products(sales_df)

    assert expected.collect() == actual.collect()


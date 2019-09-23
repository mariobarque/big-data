from tarea2.sales import get_total_by_cash_register


def get_schema():
        return ['numero_caja', 'producto', 'cantidad', 'precio_unitario', 'venta']


def test_get_total_by_cr_happy_path(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90),(1, 50)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_data_set_is_null():
    sales_df = None
    actual = get_total_by_cash_register(sales_df)

    assert actual is None

def test_get_total_by_cr_when_only_one_cr(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50), (1, 'manzana', 2, 10, 10), (1, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(1, 140)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_only_one_sale(spark_session):
    sales_data = [(1, 'mango', 1, 50, 50)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(1, 50)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_quantity_is_zero_should_ignore(spark_session):
    sales_data = [(1, 'mango', 0, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_quantity_is_negative_should_ignore(spark_session):
    sales_data = [(1, 'mango', -1, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_quantity_is_null_should_ignore(spark_session):
    sales_data = [(1, 'mango', None, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_product_is_null_should_ignore(spark_session):
    sales_data = [(1, None, 0, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_product_is_empty_should_ignore(spark_session):
    sales_data = [(1, None, 0, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_cr_is_zero_should_ignore(spark_session):
    sales_data = [(0, 'mango', 0, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_cr_is_negative_should_ignore(spark_session):
    sales_data = [(-1, 'mango', 0, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_cr_is_null_should_ignore(spark_session):
    sales_data = [(None, 'mango', 0, 50, 50), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_sale_is_null_should_ignore(spark_session):
    sales_data = [(None, 'mango', 0, 50, None), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_sale_is_zero_should_ignore(spark_session):
    sales_data = [(None, 'mango', 0, 50, 0), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()

def test_get_total_by_cr_when_sale_is_negative_should_ignore(spark_session):
    sales_data = [(None, 'mango', 0, 50, -1), (2, 'manzana', 2, 10, 10), (2, 'mango', 2, 40, 80)]
    sales_df = spark_session.createDataFrame(sales_data, get_schema())

    expected = spark_session.createDataFrame([(2, 90)], ['numer_caja', 'ventas'])
    actual = get_total_by_cash_register(sales_df)

    assert expected.collect() == actual.collect()
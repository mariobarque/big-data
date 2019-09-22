import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from functools import reduce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType
import pandas as pd


def get_json_schema():
    schema = StructType([
        StructField('numero_caja', LongType(), True),
        StructField('compras', ArrayType(
            ArrayType(
                StructType([
                    StructField('nombre', StringType(), True),
                    StructField('cantidad', IntegerType(), True),
                    StructField('precio_unitario', LongType(), True),
                ]), True
            ), True
        ), True)
    ])

    return schema


def load_data(spark, filename, schema):
    sales_df = spark \
        .read \
        .format('json') \
        .option('path', filename) \
        .schema(schema) \
        .load()


    return sales_df

def flat_sales_data(sales_unstructured):
    sales = sales_unstructured.select(
        funcs.col('numero_caja'),
        funcs.explode('compras').alias('compras_flat'),
    ).select(
        funcs.col('numero_caja'),
        funcs.explode('compras_flat').alias('compras'))

    sales = sales.select(
        funcs.col('numero_caja'),
        funcs.col('compras.nombre').alias('producto'),
        funcs.col('compras.cantidad').alias('cantidad'),
        funcs.col('compras.precio_unitario').alias('precio_unitario'),
    )

    sales = sales.withColumn('venta', sales['cantidad'] * sales['precio_unitario'])

    return sales

def union_sales_data(spark, schema, filenames):
    sales_dfs = []
    for filename in filenames:
        sales_json = load_data(spark, filename, schema)
        sales_df = flat_sales_data(sales_json)
        sales_dfs.append(sales_df)

    all_sales_df = reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), sales_dfs)

    return all_sales_df

def get_file_array():
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=argparse.FileType('r'), nargs='+', help='file name regular expression')
    args = parser.parse_args()

    return [file.name for file in args.files]

def get_total_by_products(sales_df):
    sales_group = sales_df.groupby('producto').agg({'venta': 'sum'})
    sales_group = sales_group.orderBy(sales_group['sum(venta)'].desc())

    return sales_group.select(funcs.col("producto"),
                              funcs.col("sum(venta)").alias("ventas"))

def get_total_by_cash_register(sales_df):
    sales_group = sales_df.groupby('numero_caja').agg({'venta': 'sum'})
    sales_group = sales_group.orderBy(sales_group['sum(venta)'].desc())

    return sales_group.select(funcs.col("numero_caja"),
                              funcs.col("sum(venta)").alias("ventas"))


def get_cash_register_with_most_sales(sales_df):
    sales_group = get_total_by_cash_register(sales_df)
    cr_with_most_sales = sales_group.orderBy(sales_group['ventas'].desc()).first().numero_caja

    return cr_with_most_sales

def get_cash_register_with_least_sales(sales_df):
    sales_group = get_total_by_cash_register(sales_df)
    cr_with_most_sales = sales_group.orderBy(sales_group['ventas'].asc()).first().numero_caja

    return cr_with_most_sales

def get_product_with_more_units_sold(sales_df):
    sales_group = sales_df.groupby('producto').agg({'cantidad': 'sum'})
    product_most_unit_sold = sales_group.orderBy(sales_group['sum(cantidad)'].desc()).first().producto

    return product_most_unit_sold

def get_product_with_more_sales(sales_df):
    sales_group = get_total_by_products(sales_df)
    product_most_unit_sold = sales_group.orderBy(sales_group['ventas'].desc()).first().producto

    return product_most_unit_sold

def get_sales_percentile(sales_df, percentile):
    total_registers_df = get_total_by_cash_register(sales_df)

    return total_registers_df\
        .selectExpr('percentile(ventas, ' + str(percentile) + ') as percentile')\
        .first().percentile

def get_all_metrics(sales_df):
    data = [
        ['caja_con_mas_ventas', get_cash_register_with_most_sales(sales_df)],
        ['caja_con_menos_ventas', get_cash_register_with_least_sales(sales_df)],
        ['percentil_25_por_caja', get_sales_percentile(sales_df, 0.25)],
        ['percentil_50_por_caja', get_sales_percentile(sales_df, 0.5)],
        ['percentil_75_por_caja', get_sales_percentile(sales_df, 0.75)],
        ['producto_mas_vendido_por_unidad', get_product_with_more_units_sold(sales_df)],
        ['producto_de_mayor_ingreso', get_product_with_more_sales(sales_df)],
    ]

    return pd.DataFrame(data)


def save_files(sales_df):
    # total_productos.csv
    get_total_by_products(sales_df).toPandas().to_csv('total_productos.csv', index=False, header=False)

    # total_cajas.csv
    get_total_by_cash_register(sales_df).toPandas().to_csv('total_cajas.csv', index=False, header=False)

    # metricas.csv
    get_all_metrics(sales_df).to_csv('metricas.csv', index=False, header=False)


if __name__ == "__main__":
    file_array = get_file_array()

    spark = SparkSession.builder.appName('database').master('local').getOrCreate()
    sales = union_sales_data(spark, get_json_schema(), file_array)

    save_files(sales)

    sales.show()

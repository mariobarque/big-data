import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from functools import reduce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType
from setuptools import glob


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


if __name__ == "__main__":
    file_array = get_file_array()

    spark = SparkSession.builder.appName('database').master('local').getOrCreate()
    sales = union_sales_data(spark, get_json_schema(), file_array)

    print(sales.count())
    sales.show()
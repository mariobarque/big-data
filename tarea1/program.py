from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from tarea1.top_students_finder import TopStudentsFinder


def main():
    spark = SparkSession.builder.appName('database').master('local').getOrCreate()
    students_df, courses_df, grades_df = load_data(spark)

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    top_three_students = top_students_finder.get_top_n_students()

    top_three_students.show()


if __name__ == "__main__":
    main()


def load_data(spark):
    students_df = spark \
        .read \
        .format("csv") \
        .option("path", "files/students.csv") \
        .option("header", False) \
        .schema(StructType([
                    StructField("id", IntegerType()),
                    StructField("name", StringType()),
                    StructField("student_major", StringType())])) \
        .load()

    courses_df = spark \
        .read \
        .format("csv") \
        .option("path", "files/courses.csv") \
        .option("header", False) \
        .schema(StructType([
                    StructField("id", IntegerType()),
                    StructField("credits", IntegerType()),
                    StructField("course_major", StringType())])) \
        .load()

    grades_df = spark \
        .read \
        .format("csv") \
        .option("path", "files/grades.csv") \
        .option("header", False) \
        .schema(StructType([
                    StructField("student_id", IntegerType()),
                    StructField("course_id", IntegerType()),
                    StructField("grade", IntegerType())])) \
        .load()

    return students_df, courses_df, grades_df
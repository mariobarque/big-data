from pyspark.sql import SparkSession
from tarea1.data_validations import DataValidations
import pyspark.sql.functions as func


class TopStudentsFinder:

    def __init__(self, students_df, courses_df, grades_df):
        self.students_df = students_df
        self.courses_df = courses_df
        self.grades_df = grades_df

    def join_student_data(self):
        if self.students_df is None or self.courses_df is None or self.grades_df is None:
            return None

        student_filtered_df = self.students_df.filter(DataValidations.get_student_validations())
        courses_filtered_df = self.courses_df.filter(DataValidations.get_courses_validations())
        grades_filtered_df = self.grades_df.filter(DataValidations.get_grades_validations())

        tmp_student_grades = grades_filtered_df\
            .join(student_filtered_df, grades_filtered_df['student_id'] == student_filtered_df['id'], 'inner')

        student_grades_by_course = tmp_student_grades\
            .join(courses_filtered_df, tmp_student_grades['course_id'] == courses_filtered_df['id'])\
            .select('student_id', 'name', 'course_id', 'credits', 'grade', 'student_major')

        student_grades_by_course = student_grades_by_course.sort(student_grades_by_course['name'],
                                                                 student_grades_by_course['grade'])

        return student_grades_by_course

    @staticmethod
    def group_student_data(df_students):
        if df_students is None:
            return None

        df_students_filtered = df_students.filter(DataValidations.get_joined_data_validations())

        df_students_filtered = df_students_filtered\
            .withColumn('grad_times_credits', df_students_filtered['grade'] * df_students_filtered['credits'])

        grouped_df = df_students_filtered\
            .groupby('student_id', 'student_major')\
            .agg({'credits': 'sum', 'grad_times_credits': 'sum'})

        grouped_df = grouped_df\
            .withColumn('score', func.round(grouped_df['sum(grad_times_credits)'] / grouped_df['sum(credits)'], 2)) \
            .select('student_id', 'student_major', 'score')\
            .sort(grouped_df['student_major'], grouped_df['student_id'])

        return grouped_df

    def get_top_n_students(self):
        student_df = self.join_student_data()
        grouped_df = TopStudentsFinder.group_student_data(student_df)







###################################################################################################
###################################################################################################
###################################DELETE AFTER, TESTING ONLY######################################
###################################################################################################
###################################################################################################

def test():
    spark_session = SparkSession.builder.appName('pytest-local-spark').master('local').getOrCreate()

    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, 8, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    df = TopStudentsFinder.group_student_data(df)

    df.show()

#test()
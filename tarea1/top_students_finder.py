from pyspark.sql import SparkSession, Window
from tarea1.data_validations import DataValidations
import pyspark.sql.functions as func

class TopStudentsFinder:

    def __init__(self, students_df, courses_df, grades_df):
        """
        Initialize the TopStudentsFinder class
        :param students_df: The students data frame
        :param courses_df: The courses data frame
        :param grades_df: The grades data frame
        """
        self.students_df = students_df
        self.courses_df = courses_df
        self.grades_df = grades_df

    def join_student_data(self):
        """
        Join the three data frames into one
        :return: A joined data frame with the following columns:
                student_id, name, course_id, credits, grade, student_major
        """
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
        """
        From the joined data frame of students, creates a new data frame with grouped data
        by student_id and student_major
        :param df_students: The joined data frame of students
        :return: A grouped data frame by student_id and student_major
        """
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

    @staticmethod
    def get_top_n_students_internal(student_df, grouped_df, n):
        """
        Get the top n students by major out of the joined data
        :param student_df: The student data frame
        :param grouped_df: The student data grouped
        :param n: The number of best students by major
        :return: A data frame with the top n students by major
        """

        # Do some validations
        if n <= 0:
            return None

        df = grouped_df.filter(DataValidations.get_grouped_data_validations())

        # Get only the top n highest scores
        window = Window.partitionBy(df['student_major']).orderBy(df['score'].desc())
        df = df.select('*', func.row_number().over(window).alias('row_number'))\
            .filter(func.col('row_number') <= n)\
            .selectExpr('student_id', 'student_major as major', 'score')

        # Join it with student data so that the name can be shown, and sort it
        df = df.join(student_df, df['student_id'] == student_df['id'], 'inner')\
            .select('student_id','name', 'major', 'score')\
            .orderBy('student_major', 'score', ascending=False)

        return df

    def get_top_n_students(self, n):
        """
        Get the top n students by major out of the joined data
        :param n: The number of best students by major
        :return: A data frame with the top n students by major
        """
        joined_df = self.join_student_data()
        df = TopStudentsFinder.group_student_data(joined_df)
        return TopStudentsFinder.get_top_n_students_internal(self.students_df, df, n)




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
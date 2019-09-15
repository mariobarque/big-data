from pyspark.sql import SparkSession
from tarea1.data_validations import DataValidations


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
            .select('student_id', 'name', 'course_id', 'credits', 'grade', 'course_major')

        student_grades_by_course = student_grades_by_course.sort(student_grades_by_course['name'],
                                                                 student_grades_by_course['grade'])

        return student_grades_by_course

    def group_student_data(self):
        return None

    def get_top_n_students(self):
        pass





###################################################################################################
###################################################################################################
###################################DELETE AFTER, TESTING ONLY######################################
###################################################################################################
###################################################################################################

def test():
    spark_session = SparkSession.builder.appName('pytest-local-spark').master('local').getOrCreate()

    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business'), (4, 6, 'CS'), (5, 8, 'CS')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84), (1, 4, 65), (1, 5, 87)]

    students_df = spark_session.createDataFrame([], ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    actual_df.show()

#test()
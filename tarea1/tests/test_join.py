

def test_join_happy_path(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 2), (3, 4, 3)]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'major'])
    course_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'major'])
    grade_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [('Mario', 1, 4, 90, 'CS'), ('Paula', 2, 2, 95, 'Biology'), ('Juan', 3, 4, 84, 'Business')]
    expected_df = spark_session.createDataFrame(expected_data, ['name', 'course_id', 'credits', 'grade', 'major'])

    pass
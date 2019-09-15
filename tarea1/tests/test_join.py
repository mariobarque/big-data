from pyspark.sql.types import StructField, IntegerType, StructType, StringType

from tarea1.top_students_finder import TopStudentsFinder


###########################GENERAL TESTS###########################

def test_join_happy_path(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (1, 'Mario', 1, 4, 90, 'CS'),
                     (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_students_null(spark_session):
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = None
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert actual_df is None

def test_join_when_courses_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = None
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert actual_df is None

def test_join_when_grades_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = None

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert actual_df is None

def test_join_when_no_students(spark_session):
    student_data = []
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business'), (4, 6, 'CS'), (5, 8, 'CS')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84), (1, 4, 65), (1, 5, 87)]

    field = [StructField('id', IntegerType(), True),
             StructField('name', IntegerType(), True),
             StructField('student_major', StringType(), True)]

    students_df = spark_session.createDataFrame(student_data, schema=StructType(field))
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert actual_df.rdd.isEmpty()

def test_join_when_when_no_courses(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = []
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84), (1, 4, 65), (1, 5, 87)]

    field = [StructField('id', IntegerType(), True),
             StructField('credits', IntegerType(), True),
             StructField('course_major', StringType(), True)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, schema=StructType(field))
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert actual_df.rdd.isEmpty()

def test_join_when_when_no_grades(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business'), (4, 6, 'CS'), (5, 8, 'CS')]
    grade_data = []

    field = [StructField('student_id', IntegerType(), True),
             StructField('course_id', IntegerType(), True),
             StructField('grade', IntegerType(), True)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, schema=StructType(field))

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert actual_df.rdd.isEmpty()

def test_join_when_more_than_one_course_per_student(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business'), (4, 6, 'CS'), (5, 8, 'CS')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84), (1, 4, 65), (1, 5, 87)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (1, 'Mario', 4, 6, 65, 'CS'), (1, 'Mario', 5, 8, 87, 'CS'),
                     (1, 'Mario', 1, 4, 90, 'CS'), (2, 'Paula', 2, 2, 95, 'Biology'),]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

###########################STUDENT###########################

def test_join_when_invalid_student_id_null(spark_session):
    student_data = [(None, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_student_id_negative(spark_session):
    student_data = [(-1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_student_name_null(spark_session):
    student_data = [(1, None, 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_student_name_empty(spark_session):
    student_data = [(1, '', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_student_major_null(spark_session):
    student_data = [(1, 'Mario', None), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_student_major_empty(spark_session):
    student_data = [(1, 'Mario', ''), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()


###########################COURSE###########################

def test_join_when_invalid_course_id_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(None, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_course_id_negative(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(-1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_course_credits_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, None, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_course_credits_negative(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, -1, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_course_major_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, None), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_course_major_empty(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, ''), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

###########################GRADES###########################

def test_join_when_invalid_grade_student_id_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(None, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_grade_student_id_negative(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(-1, 1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_grade_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business'), (4, 6, 'CS'), (5, 8, 'CS')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84), (1, 4, None), (1, 5, 87)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (1, 'Mario', 5, 8, 87, 'CS'),
                     (1, 'Mario', 1, 4, 90, 'CS'), (2, 'Paula', 2, 2, 95, 'Biology'),]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_grade_negative(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business'), (4, 6, 'CS'), (5, 8, 'CS')]
    grade_data = [(1, 1, 90), (2, 2, 95), (3, 3, 84), (1, 4, -1), (1, 5, 87)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (1, 'Mario', 5, 8, 87, 'CS'),
                     (1, 'Mario', 1, 4, 90, 'CS'), (2, 'Paula', 2, 2, 95, 'Biology'),]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_grade_negative_only_course_of_student(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, 1, -10), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology'),]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_grade_course_id_is_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, None, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()

def test_join_when_invalid_grade_course_id_is_negative(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'Business')]
    course_data = [(1, 4, 'CS'), (2, 2, 'Biology'), (3, 4, 'Business')]
    grade_data = [(1, -1, 90), (2, 2, 95), (3, 3, 84)]

    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])
    courses_df = spark_session.createDataFrame(course_data, ['id', 'credits', 'course_major'])
    grades_df = spark_session.createDataFrame(grade_data, ['student_id', 'course_id', 'grade'])

    expected_data = [(3, 'Juan', 3, 4, 84, 'Business'), (2, 'Paula', 2, 2, 95, 'Biology')]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name',
                                                                'course_id', 'credits',
                                                                'grade', 'course_major'])

    top_students_finder = TopStudentsFinder(students_df, courses_df, grades_df)
    actual_df = top_students_finder.join_student_data()

    assert expected_df.collect() == actual_df.collect()
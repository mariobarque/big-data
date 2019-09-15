from tarea1.top_students_finder import TopStudentsFinder


def test_group_happy_path(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, 8, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0), (1, 'CS', 80.33)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_when_student_data_is_null():
    df = None
    actual_df = TopStudentsFinder.group_student_data(df)

    assert actual_df is None

def test_group_only_one_course_per_student(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0), (1, 'CS', 65.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_student_id_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (None, 'Mario', 4, 6, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_student_id_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (-1, 'Mario', 4, 6, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_name_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, None, 4, 6, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_name_empty(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, '', 4, 6, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_course_id_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', None, 6, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_course_id_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', -1, 6, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_credits_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, None, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_credits_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, -1, 65, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_grade_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, 6, None, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_grade_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, 6, -1, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_major_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, 6, 65, None),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_only_one_course_per_student_major_empty(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (1, 'Mario', 4, 6, 65, ''),
            (2, 'Paula', 2, 2, 95, 'Biology'),]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 95.0), (3, 'Business', 84.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, 8, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 80.33)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_student_id_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (None, 'Mario', 5, 8, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_student_id_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (-1, 'Mario', 5, 8, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_student_major_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, 8, 87, None),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_student_major_empty(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, 8, 87, ''),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_credits_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, None, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_credits_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, -1, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_course_id_null(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', None, 8, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_course_id_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', -1, 8, 87, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_grade_none(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, 8, None, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
            ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

def test_group_multiple_courses_multiple_students_grade_negative(spark_session):
    data = [(3, 'Juan', 3, 4, 84, 'Business'),
            (3, 'Juan', 6, 2, 90, 'Business'),
            (3, 'Juan', 7, 8, 40, 'Business'),
            (1, 'Mario', 4, 6, 65, 'CS'),
            (1, 'Mario', 5, 8, -1, 'CS'),
            (1, 'Mario', 1, 4, 90, 'CS'),
            (2, 'Paula', 2, 2, 95, 'Biology'),
            (2, 'Paula', 2, 4, 50, 'Biology'),
            (2, 'Paula', 2, 4, 75, 'Biology')]
    df = spark_session.createDataFrame(data,
                ['student_id', 'name', 'course_id', 'credits', 'grade', 'student_major'])

    expected_data = [(2, 'Biology', 69.0), (3, 'Business', 59.71), (1, 'CS', 75.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'student_major','score'])

    actual_df = TopStudentsFinder.group_student_data(df)

    assert expected_df.collect() == actual_df.collect()

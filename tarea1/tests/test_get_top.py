from tarea1.top_students_finder import TopStudentsFinder


def test_get_top_happy_path(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'),
                    (4, 'Marcela', 'CS'), (5, 'Carlos', 'Biology'), (6, 'Ivan', 'CS'),
                    (7, 'Pablo', 'Biology'), (8, 'Karla', 'Biology'), (9, 'Pedro', 'Biology'),]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, 'CS', 95.0), (5, 'Biology', 87.0),
                    (6, 'CS', 84.0), (7, 'Biology', 92.0), (8, 'Biology', 93.0), (9, 'Biology', 83.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    expected_data = [(4, 'Marcela', 'CS', 95.0), (3, 'Juan', 'CS', 91.0),
                     (8, 'Karla', 'Biology', 93.0), (7, 'Pablo', 'Biology', 92.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name', 'major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 2)

    assert  expected_df.collect() == actual_df.collect()

def test_get_top_when_top_one(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'),
                    (4, 'Marcela', 'CS'), (5, 'Carlos', 'Biology'), (6, 'Ivan', 'CS'),
                    (7, 'Pablo', 'Biology'), (8, 'Karla', 'Biology'), (9, 'Pedro', 'Biology'),]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, 'CS', 95.0), (5, 'Biology', 87.0),
                    (6, 'CS', 84.0), (7, 'Biology', 92.0), (8, 'Biology', 93.0), (9, 'Biology', 83.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    expected_data = [(4, 'Marcela', 'CS', 95.0),
                     (8, 'Karla', 'Biology', 93.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name', 'major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 1)

    assert  expected_df.collect() == actual_df.collect()

def test_get_top_when_top_is_zero(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'),
                    (4, 'Marcela', 'CS'), (5, 'Carlos', 'Biology'), (6, 'Ivan', 'CS'),
                    (7, 'Pablo', 'Biology'), (8, 'Karla', 'Biology'), (9, 'Pedro', 'Biology'),]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, 'CS', 95.0), (5, 'Biology', 87.0),
                    (6, 'CS', 84.0), (7, 'Biology', 92.0), (8, 'Biology', 93.0), (9, 'Biology', 83.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 0)

    assert  actual_df is None

def test_get_top_when_top_is_negative(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'),
                    (4, 'Marcela', 'CS'), (5, 'Carlos', 'Biology'), (6, 'Ivan', 'CS'),
                    (7, 'Pablo', 'Biology'), (8, 'Karla', 'Biology'), (9, 'Pedro', 'Biology'),]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, 'CS', 95.0), (5, 'Biology', 87.0),
                    (6, 'CS', 84.0), (7, 'Biology', 92.0), (8, 'Biology', 93.0), (9, 'Biology', 83.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, -1)

    assert  actual_df is None

def test_get_top_when_top_greater_than_actual_data(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'), (4, 'Carlos', 'Biology')]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, 'Biology', 87.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    expected_data = [(3, 'Juan', 'CS', 91.0),(1, 'Mario', 'CS', 76.0),
                     (2, 'Paula', 'Biology', 88.0), (4, 'Carlos', 'Biology', 87.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name', 'major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 3)

    assert  expected_df.collect() == actual_df.collect()

def test_get_top_when_major_doesnt_have_data(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'), (4, 'Carlos', 'Biology')]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (3, 'CS', 91.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    expected_data = [(3, 'Juan', 'CS', 91.0),(1, 'Mario', 'CS', 76.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name', 'major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 2)

    assert  expected_df.collect() == actual_df.collect()

def test_get_top_when_score_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'),
                    (4, 'Marcela', 'CS'), (5, 'Carlos', 'Biology'), (6, 'Ivan', 'CS'),
                    (7, 'Pablo', 'Biology'), (8, 'Karla', 'Biology'), (9, 'Pedro', 'Biology'),]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, 'CS', None), (5, 'Biology', 87.0),
                    (6, 'CS', 84.0), (7, 'Biology', 92.0), (8, 'Biology', 93.0), (9, 'Biology', 83.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    expected_data = [(3, 'Juan', 'CS', 91.0), (6, 'Ivan', 'CS', 84.0),
                     (8, 'Karla', 'Biology', 93.0), (7, 'Pablo', 'Biology', 92.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name', 'major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 2)

    assert  expected_df.collect() == actual_df.collect()

def test_get_top_when_major_null(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'),
                    (4, 'Marcela', 'CS'), (5, 'Carlos', 'Biology'), (6, 'Ivan', 'CS'),
                    (7, 'Pablo', 'Biology'), (8, 'Karla', 'Biology'), (9, 'Pedro', 'Biology'),]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, None, 95.0), (5, 'Biology', 87.0),
                    (6, 'CS', 84.0), (7, 'Biology', 92.0), (8, 'Biology', 93.0), (9, 'Biology', 83.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    expected_data = [(3, 'Juan', 'CS', 91.0), (6, 'Ivan', 'CS', 84.0),
                     (8, 'Karla', 'Biology', 93.0), (7, 'Pablo', 'Biology', 92.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name', 'major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 2)

    assert  expected_df.collect() == actual_df.collect()

def test_get_top_when_major_empty(spark_session):
    student_data = [(1, 'Mario', 'CS'), (2, 'Paula', 'Biology'), (3, 'Juan', 'CS'),
                    (4, 'Marcela', 'CS'), (5, 'Carlos', 'Biology'), (6, 'Ivan', 'CS'),
                    (7, 'Pablo', 'Biology'), (8, 'Karla', 'Biology'), (9, 'Pedro', 'Biology'),]
    students_df = spark_session.createDataFrame(student_data, ['id', 'name', 'student_major'])

    grouped_data = [(1, 'CS', 76.0), (2, 'Biology', 88.0), (3, 'CS', 91.0), (4, '', 95.0), (5, 'Biology', 87.0),
                    (6, 'CS', 84.0), (7, 'Biology', 92.0), (8, 'Biology', 93.0), (9, 'Biology', 83.0)]
    grouped_df = spark_session.createDataFrame(grouped_data, ['student_id', 'student_major', 'score'])

    expected_data = [(3, 'Juan', 'CS', 91.0), (6, 'Ivan', 'CS', 84.0),
                     (8, 'Karla', 'Biology', 93.0), (7, 'Pablo', 'Biology', 92.0)]
    expected_df = spark_session.createDataFrame(expected_data, ['student_id', 'name', 'major', 'score'])

    actual_df = TopStudentsFinder.get_top_n_students_internal(students_df, grouped_df, 2)

    assert  expected_df.collect() == actual_df.collect()




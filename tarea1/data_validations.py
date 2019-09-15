class DataValidations:
    @staticmethod
    def get_student_validations():
        validations = [
            'id is not null',
            'id > 0',
            'name is not null',
            'length(name) > 0',
            'student_major is not null',
            'length(student_major) > 0'
        ]

        return ' and '.join(validations)

    @staticmethod
    def get_courses_validations():
        validations = [
            'id is not null',
            'id > 0',
            'credits is not null',
            'credits > 0',
            'course_major is not null',
            'length(course_major) > 0'
        ]

        return ' and '.join(validations)

    @staticmethod
    def get_grades_validations():
        validations = [
            'student_id is not null',
            'student_id > 0',
            'course_id is not null',
            'course_id > 0',
            'grade is not null',
            'grade > 0'
        ]

        return ' and '.join(validations)

    @staticmethod
    def get_joined_data_validations():
        validations = [
            'student_id is not null',
            'student_id > 0',
            'name is not null',
            'length(name) > 0',
            'course_id is not null',
            'course_id > 0',
            'credits is not null',
            'credits > 0',
            'grade is not null',
            'grade > 0',
            'student_major is not null',
            'length(student_major) > 0'
        ]
        return ' and '.join(validations)
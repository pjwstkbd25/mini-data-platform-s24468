# import pandas as pd
# import numpy as np
# from custom_random_generators import random_name, random_age, random_grade, random_score
#
# def generate_random_educational_data(num_rows):
#     data = {
#         # "StudentID": np.random.randint(10000, 99999, size=num_rows),
#         "Full Name": [random_name() for _ in range(num_rows)],
#         "Age": [random_age() for _ in range(num_rows)],
#         "Grade": [random_grade() for _ in range(num_rows)],
#         "MathScore": [random_score() for _ in range(num_rows)],
#         "EnglishScore": [random_score() for _ in range(num_rows)],
#         "ScienceScore": [random_score() for _ in range(num_rows)],
#         "HistoryScore": [random_score() for _ in range(num_rows)]
#     }
#     return pd.DataFrame(data)
# def insert_random_educational_data(engine, table_name, num_rows):
#     data = generate_random_educational_data(num_rows)
#     data.to_sql(
#         name=table_name,
#         con=engine,
#         if_exists='append',  # Use 'append' to add to the existing table
#         index=False
#     )
import pandas as pd
import numpy as np
import random
import string

def random_name():
    first_names = ["John", "Jane", "Ahmed", "Maria", "Chen", "Carlos", "Elena", "Raj", "Aisha", "Tom"]
    last_names = ["Doe", "Smith", "Khan", "Garcia", "Wei", "Rodriguez", "Ivanov", "Singh", "Williams", "Taylor"]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def random_age():
    return random.randint(18, 60)  # Random age between 18 and 60

def random_education_level():
    return random.choice(["Primary", "Secondary", "Tertiary"])  # Education levels

def random_major():
    return random.choice([
        "Computer Science", "Business Administration", "Engineering", "Biology",
        "Psychology", "Economics", "History", "Nothing"  # Some people might not have a major
    ])

def random_years():
    start_year = random.randint(1990, 2020)  # Education start between 1990 and 2020
    end_year = start_year + random.randint(2, 6)  # Random study duration
    return start_year, end_year

def random_institution_type():
    return random.choice(["Public", "Private"])

def random_grade():
    return round(random.uniform(0.0, 4.0), 1)  # GPA-like system (0.0 - 4.0)

def generate_random_educational_data(num_rows):
    data = {
        "Full Name": [random_name() for _ in range(num_rows)],
        "Age": [random_age() for _ in range(num_rows)],
        "Education Level": [random_education_level() for _ in range(num_rows)],
        "Major": [random_major() for _ in range(num_rows)],
        "Year Started Education": [],
        "Year Completed Education": [],
        "Type of Educational Institution": [random_institution_type() for _ in range(num_rows)],
        "Average Grade": [random_grade() for _ in range(num_rows)]
    }

    # Generate Year Started and Year Completed values
    for _ in range(num_rows):
        start, end = random_years()
        data["Year Started Education"].append(start)
        data["Year Completed Education"].append(end)

    return pd.DataFrame(data)

def insert_random_educational_data(engine, table_name, num_rows):
    data = generate_random_educational_data(num_rows)
    data.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False
    )

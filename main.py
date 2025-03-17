import pandas as pd
from sqlalchemy import create_engine, Integer, Float, Text, DateTime
from sqlalchemy.engine import reflection
from RandomData import insert_random_educational_data


def get_engine():
    user = "Jarek"
    password = "Jarek"
    host = "localhost"
    database = "data"
    return create_engine(f'postgresql://{user}:{password}@{host}/{database}')


def infer_data_type(val):
    if pd.api.types.is_integer_dtype(val):
        return Integer()
    elif pd.api.types.is_float_dtype(val):
        return Float()
    elif pd.api.types.is_datetime64_any_dtype(val):
        return DateTime()
    else:
        return Text()


def create_and_insert_data(file_path, table_name):
    """Creates a table and inserts data only if the table does not exist."""
    engine = get_engine()

    if check_table_exists(engine, table_name):
        print(f"Skipping table creation for {table_name} (already exists).")
        return  # If table exists, skip creation

    print(f"Creating and inserting data for {table_name}...")
    data = pd.read_csv(file_path)

    dtype_mapping = {
        col: infer_data_type(data[col].dropna().iloc[0])
        for col in data.columns
    }

    with engine.connect() as connection:
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
        )


def check_table_exists(engine, table_name):
    """Checks if a table exists in the database."""
    inspector = reflection.Inspector.from_engine(engine)
    return table_name in inspector.get_table_names()


if __name__ == '__main__':
    engine = get_engine()

    # Only create and insert data if the tables do not exist
    create_and_insert_data('Dataset/EducationalData.csv', 'educational_data')
    create_and_insert_data('Dataset/AddictionData.csv', 'addiction_data')
    create_and_insert_data('Dataset/InvestmentData.csv', 'investment_data')

    # Insert random data (this can always run)
    insert_random_educational_data(engine, 'educational_data', 10)

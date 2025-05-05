import time
import pandas as pd
from sqlalchemy import create_engine, Integer, Float, Text, DateTime, Boolean, text
from RandomData import insert_random_educational_data

def get_engine():
    user = "Jarek"
    password = "Jarek"
    host = "db"       # changed to Docker container hostname for PostgreSQL
    port = 5432       # default Postgres port inside container
    database = "data"
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

def infer_data_type(series: pd.Series):
    """
    Determine the SQLAlchemy type for an entire Pandas series (column).
    This uses the dtype of the series (not a single value) to avoid misclassification.
    - Booleans -> Boolean
    - Integers -> Integer
    - Floats   -> Float
    - Date/time -> DateTime
    - Otherwise -> Text (treat as text/varchar)
    """
    if pd.api.types.is_bool_dtype(series):
        return Boolean()
    if pd.api.types.is_integer_dtype(series):
        return Integer()
    if pd.api.types.is_float_dtype(series):
        return Float()
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime()
    # default: treat as text
    return Text()

def create_and_insert_data(csv_path, table_name):
    """Creates a table and inserts data from a CSV, then adds a primary key if table is new."""
    engine = get_engine()
    # Wait until the database is ready to accept connections
    print(f"Connecting to database to create {table_name}...")
    for attempt in range(30):
        try:
            with engine.connect() as _:
                break  # connection successful
        except Exception as e:
            print("Database not ready, waiting...")
            time.sleep(1)
    else:
        raise RuntimeError("Database not available after 30 seconds.")
    print(f"Database connection established. Creating and inserting data for '{table_name}'...")

    # Read CSV data into pandas DataFrame
    data = pd.read_csv(csv_path)
    dtype_mapping = {col: infer_data_type(data[col]) for col in data.columns}

    # Create table and insert data (if table exists, replace it)
    data.to_sql(name=table_name, con=engine, if_exists='replace', index=False, dtype=dtype_mapping)
    # Add a serial primary key column named 'id'
    with engine.connect() as connection:
        connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN id SERIAL PRIMARY KEY;"))
    print(f"Table '{table_name}' created and CSV data inserted.")

if __name__ == '__main__':
    engine = get_engine()
    # create_and_insert_data('Dataset/EducationalData.csv', 'educational_data')
    create_and_insert_data('educational_data.csv', 'educational_data')
    insert_random_educational_data(engine, 'educational_data', 10)
    print("Random data inserted into 'educational_data'.")

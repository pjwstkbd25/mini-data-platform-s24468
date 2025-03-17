import pandas as pd
from sqlalchemy import create_engine, Integer, Float, Text, DateTime


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
    data = pd.read_csv(file_path)

    dtype_mapping = {
        col: infer_data_type(data[col].dropna().iloc[0])
        for col in data.columns
    }

    engine = get_engine()

    with engine.connect() as connection:
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
        )


create_and_insert_data('Dataset/EducationalData.csv', 'educational_data')
create_and_insert_data('Dataset/AddictionData.csv', 'addiction_data')
create_and_insert_data('Dataset/InvestmentData.csv', 'investment_data')

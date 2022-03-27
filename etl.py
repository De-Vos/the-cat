import pandas as pd
import numpy as np

import databases.sql as sql
import databases.no_sql as no_sql


def extract(file_path):
    
    return pd.read_csv(file_path)

def transform_reservations(df):
    
    df_str = df.astype(str)
    df_str = df_str.drop(columns=['Unnamed: 0'])

    columns = list(df_str)

    for col in columns:
        df_str[col] = np.where(df_str[col].str.contains("[A-Za-z]", regex=True), None, df_str[col])

    # TODO: calculate city null values through the provided coordinates

    return df_str


def transform_locations(df):

    df_str = df.astype(str) 
    return df_str.drop(columns=['Unnamed: 0'])



def load(df):

    load_to_sql(df)
    load_to_no_sql(df)   


def load_to_sql(df, table, schema):

    psql = sql.Postgres()
    psql.write_df(df, table, schema)


def load_to_no_sql(df, db): 

    r = no_sql.Redis()
    r.set_df(df, db)



def etl_reservations():

    df_raw = extract('data/assignment_reservations.csv')
    df_clean = transform_reservations(df_raw)
    df_clean.to_csv('clearn_res.csv')
    load_to_sql(df_clean, table="reservations", schema='felyx')
    load_to_no_sql(df_clean, db='table_reservations')

    print("reservation etl successfully completed")


def etl_locations():

    df_raw = extract('data/assignment_locations.csv')
    df_clean = transform_locations(df_raw)
    load_to_sql(df_clean, table="locations", schema='felyx')
    load_to_no_sql(df_clean, db='table_locations')

    print("location etl successfully completed")



if __name__ == '__main__':

    etl_locations()
    etl_reservations()



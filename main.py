# ingestion script from csv to psql

# 1. import libraries needed
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")

try:
    conn = psycopg2.connect(
        host=db_host, dbname=db_name, user=db_user, port=db_port, password=db_password
    )
except psycopg2.Error as e:
    print("Error: Could not make a connection to the Postgres db")
    print(e)
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the postgre db")
    print(e)

conn.set_session(autocommit=True)

movies = pd.read_csv(r"C:\Users\Darko\DataIngestion\tables\movies.csv")
links = pd.read_csv(r"C:\Users\Darko\DataIngestion\tables\links.csv")
ratings = pd.read_csv(r"C:\Users\Darko\DataIngestion\tables\ratings.csv")
tags = pd.read_csv(r"C:\Users\Darko\DataIngestion\tables\tags.csv")


def get_tables(**kwargs):
    """Description: This function is to get the table names and column titles of an arbitrary number of dataframes with the names as keyword arguments

    Args: [table_name] = [pandas_df],[table_name2] = [pandas_df2]


    Returns: dictionary of table name prefixing "Columns_tiles" as key and corresponding column titles as values

    """
    all_column_titles = []

    for df_name, df in kwargs.items():
        table_name = f"{df_name}_column_titles"
        column_title_dict = {table_name: df.columns.to_list()}
        all_column_titles.append(column_title_dict)

    return all_column_titles


# get_tables(movie = movies,link = links, rating = ratings,tag = tags)
table_columns = get_tables(movie=movies, link=links, rating=ratings, tag=tags)

# columns =
# create_table = f"DROP TABLE IF EXISTS {name} CREATE TABLE {name}({columns});"


try:
    with conn.cursor() as cur:
        print(f"Connecting to : {db_name}")
        cur.execute("SELECT * FROM RATINGS WHERE rating in (4.2,4.3,4.5)")
        rs = cur.fetchmany(size=20,)
        df = pd.DataFrame(rs)
        ##df.columns = rs.keys()
        # print (df.head())
except psycopg2.Error as e:
    print("Error: Could not get cursor to the postgre db")
    print(e)

else:
    print(f'operation successful with {db_name}')
# else:
#     print(f'Unable to insert values into {db_name}')
df.columns = (table_columns[2]['rating_column_titles'])



print(df)
print(f"Disconnected from {db_name} successfuly")

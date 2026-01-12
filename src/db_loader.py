from pathlib import Path
from psycopg2.extensions import connection as Connection, cursor as Cursor
from typing import List, Dict, Any
from pandas import DataFrame
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv
import os

load_dotenv()

ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"

FILE_NAME = "movies.csv"

def read(file_name : str) -> DataFrame:
    # Responsible for reading the data and handling type integrity

    # Pathing
    path = DATA_DIR / file_name
    data = pd.read_csv(path)

    # Drop duplicates and NULL values
    data = data.drop(columns=["Poster_Link"], axis = 'columns')
    data = data.drop_duplicates()
    
    data['Released_Year'] = pd.to_numeric(data['Released_Year'], errors="coerce")
    data = data.dropna(subset=["Series_Title","Director", "Released_Year"])

    # Filling NULL values
    data['Certificate'] = data['Certificate'].fillna("Not Rated")
    data['Overview'] = data['Overview'].fillna("")
    data['Gross'] = data['Gross'].fillna(0)
    data['Meta_score'] = data['Meta_score'].fillna(-1) # -1 Symbolizes not rated yet, not a negative score

    # Fixing types
    data['Released_Year'] = data['Released_Year'].astype('int64')
    for col in ["Series_Title","Certificate","Overview","Director","Star1","Star2","Star3","Star4"]:
        data[col] = data[col].astype('string')

    # Formatting
    data['Runtime'] = data['Runtime'].astype(str).str.replace(" min","").astype('int64')
    data['Gross'] = data['Gross'].astype(str).str.replace(",","").astype('int64')
    data['Genre'] = data['Genre'].astype(str).str.split(", ")

    return data


def load_data(cur : Cursor, data : DataFrame):
    
    movies_data = data[["Serial_Title", "Released_Year", "Certificate", "Runtime", "Overview", "Meta_score", "IMDB_Rating", "No_of_Votes","Gross"]]
    print(movies_data)

    pass

def verify_load(cur : Cursor):
    # Just a few queries to test that data has been loaded
    pass

def connect_db() -> Connection:
    
    connection = psy.connect(
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        host = os.getenv("DB_HOST")
    )

    return connection

def db_setup(file_name : str = FILE_NAME):
    # Basically everything that main does without test queries , so that it can be called in main.py
    data = read(file_name)
    print(data.columns)
    connection = connect_db()
    cur = connection.cursor()

    load_data(cur, data)

if __name__ == "__main__":
    # Check the connections and the functions
    db_setup()


    
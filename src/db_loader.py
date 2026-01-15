from pathlib import Path
from psycopg2.extensions import connection as Connection, cursor as Cursor
from typing import List, Dict, Any, Tuple
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

    # Path error handling
    if not path.exists():
        raise FileNotFoundError(f"{file_name} not found in data directory")
    data = pd.read_csv(path)


    # Drop duplicates and NULL values
    data = data.drop(columns=["Poster_Link"], axis = 'columns')
    data = data.drop_duplicates()

    # Checking for presence of required columns
    REQUIRED_COLUMNS = ["Series_Title","Director","Released_Year","Genre"]
    missing = [col for col in REQUIRED_COLUMNS if col not in data.columns]
    if missing:
        raise ValueError(f"Missing columns :{missing}")
    
    # Handling non-numeric values in year column
    data['Released_Year'] = pd.to_numeric(data['Released_Year'], errors="coerce")
    data = data.dropna(subset=["Series_Title","Director", "Released_Year","Genre"])

    # Filling NULL values
    data['Certificate'] = data['Certificate'].fillna("Not Rated")
    data['Overview'] = data['Overview'].fillna("")
    data['Gross'] = data['Gross'].fillna(0)
    data['Meta_score'] = data['Meta_score'].fillna(0) # 0 Symbolizes not rated yet, not a zero score

    # Fixing types
    data['Released_Year'] = data['Released_Year'].astype('int64')
    for col in ["Series_Title","Certificate","Overview","Director","Star1","Star2","Star3","Star4","Genre"]:
        data[col] = data[col].astype(str)
    
    # Formatting
    data['Runtime'] = data['Runtime'].astype(str).str.replace(" min","").astype('int64')
    data['Gross'] = data['Gross'].astype(str).str.replace(",","").astype('int64')
    data['Genre'] = data['Genre'].str.replace(" ","").str.split(',')
    data['Stars'] = data['Star1'] + ',' + data['Star2'] + ',' + data['Star3'] + ',' + data['Star4'] 
    data['Stars'] = data['Stars'].str.split(",")
    data = data.drop(columns=["Star1","Star2","Star3","Star4"])

    
    return data

def load_directors(cur : Cursor,data : DataFrame):
    # Loading the data into directors table
    data_to_insert = list(map(lambda x: (x,),data.unique()))

    INSERT_QUERY = """INSERT INTO directors(name)
    VALUES(%s) ON CONFLICT (name) DO NOTHING;
    """

    cur.executemany(INSERT_QUERY, data_to_insert)

def load_movies(cur : Cursor, data: DataFrame):
    # Loading data into movies table
    data_to_insert = data.copy()

    cur.execute("SELECT * FROM directors")
    director_data = cur.fetchall()

    # Creating mapping for Director name -> director id
    mapping = {}
    for id,name in director_data:
        mapping[name] = id

    # Converting Director names to director id
    data_to_insert["Director"] = data_to_insert["Director"].map(mapping)
    data_to_insert = data_to_insert.rename(columns={'Director':'Director_id'})
    
    # Creating list of tuples for entry
    data_tuples = list(data_to_insert.itertuples(index=False,name=None))
    
    INSERT_QUERY = """INSERT INTO movies(title,release_year, certificate, runtime, overview, meta_score, rating, votes, gross, director_id)
    VALUES(%s, %s ,%s ,%s ,%s ,%s, %s, %s, %s, %s);
    """

    cur.executemany(INSERT_QUERY,data_tuples)

def load_genres(cur: Cursor, data : DataFrame):

    genre_data = data.unique()

    # Creating list of tuples for insert
    # Using (genre,) ensures that it creates tuple of the entire word, instead of individual characters
    genres_tuples = [(genre,) for genre in genre_data]

    INSERT_QUERY = """INSERT INTO genres(genre)
    VALUES(%s);
    """

    cur.executemany(INSERT_QUERY, genres_tuples)

def load_actors(cur: Cursor, data: DataFrame):

    actors_data = data.unique()

    # Creating tuples for entry
    actors_tuples = [(actor,) for actor in actors_data]

    INSERT_QUERY = """INSERT INTO actors(name)
    VALUES(%s);
    """

    cur.executemany(INSERT_QUERY, actors_tuples)

def load_movies_genres(cur: Cursor, data: DataFrame):

    # Getting the movie_id mapping
    cur.execute("SELECT id,title FROM movies")
    data_movies = cur.fetchall()

    movie_mapping = {}
    for id, movie in data_movies:
        movie_mapping[movie] = id

    # Getting the genre_id mapping
    cur.execute("SELECT id,genre FROM genres")
    data_genres = cur.fetchall()
    genre_mapping = {}
    for id,genre in data_genres:
        genre_mapping[genre] = id


    data_to_insert = data.copy()
    data_to_insert['Series_Title'] = data_to_insert['Series_Title'].map(movie_mapping)

    data_to_insert = data_to_insert.explode('Genre', ignore_index = True)
    data_to_insert['Genre'] = data_to_insert['Genre'].map(genre_mapping)
    data_to_insert = data_to_insert.rename(columns={'Genre':'Genre_id','Series_Title':'movie_id'})    
    data_tuples = list(set(data_to_insert.itertuples(index=False, name = None)))
    
    INSERT_QUERY = """INSERT INTO movies_genres(movie_id, genre_id)
    VALUES(%s, %s)
    """

    cur.executemany(INSERT_QUERY,data_tuples)


def load_movies_actors(cur: Cursor, data: DataFrame):
    # Getting mappings
    cur.execute("SELECT id,title FROM movies")
    data_movies = cur.fetchall()

    movie_mapping = {}
    for id, movie in data_movies:
        movie_mapping[movie] = id

    cur.execute("SELECT id,name FROM actors")
    data_actors = cur.fetchall()
    actor_mapping = {}
    for id,actor in data_actors:
        actor_mapping[actor] = id

    # Applying mappings
    data_to_insert = data.copy()
    data_to_insert['Series_Title'] = data_to_insert['Series_Title'].map(movie_mapping)

    data_to_insert = data_to_insert.explode('Stars', ignore_index = True)
   
    data_to_insert['Stars'] = data_to_insert['Stars'].map(actor_mapping)
    data_to_insert = data_to_insert.rename(columns = {'Stars' : 'Actor_id','Series_Title' :'Movie_id'})
    data_tuples = list(set(data_to_insert.itertuples(index=False, name = None)))

    INSERT_QUERY = """INSERT INTO movies_actors(movie_id, actor_id)
    VALUES(%s, %s);
    """

    cur.executemany(INSERT_QUERY,data_tuples)

def reset_sequence(cur):
    cur.execute("TRUNCATE movies,directors,genres,movies_actors,movies_genres, actors;")


    RESET_QUERY = "ALTER SEQUENCE movies_id_seq RESTART WITH 1 ;"

    cur.execute("ALTER SEQUENCE movies_id_seq RESTART WITH 1 ;")
    cur.execute("ALTER SEQUENCE directors_id_seq RESTART WITH 1 ;")
    cur.execute("ALTER SEQUENCE genres_id_seq RESTART WITH 1 ;")
    cur.execute("ALTER SEQUENCE actors_id_seq RESTART WITH 1 ;")

def load_data(cur : Cursor, data : DataFrame):
    
    movies_data = data[["Series_Title", "Released_Year", "Certificate", "Runtime", "Overview", "Meta_score", "IMDB_Rating", "No_of_Votes","Gross","Director"]]

    directors_data = data["Director"].copy()

    genres_data = data['Genre'].explode()
    
    actors_data = data['Stars'].explode()

    movies_genres_data = data[["Series_Title","Genre"]].copy()

    movies_actors_data = data[["Series_Title","Stars"]].copy()

    load_directors(cur,directors_data)
    load_movies(cur, movies_data)
    load_genres(cur,genres_data)
    load_actors(cur,actors_data)
    load_movies_genres(cur,movies_genres_data)
    load_movies_actors(cur,movies_actors_data)


def verify_load(cur : Cursor):
    # Just a few queries to test that data has been loaded
    cur.execute("SELECT * FROM movies LIMIT 5")
    print(cur.fetchall())

    cur.execute("SELECT * FROM directors LIMIT 5")
    print(cur.fetchall())

    cur.execute("SELECT * FROM genres LIMIT 5")
    print(cur.fetchall())

    cur.execute("SELECT * FROM actors LIMIT 5")
    print(cur.fetchall())

    cur.execute("SELECT * FROM movies_genres LIMIT 5")
    print(cur.fetchall())

    cur.execute("SELECT * FROM movies_actors LIMIT 5")
    print(cur.fetchall())

def connect_db() -> Connection:
    
    connection = psy.connect(
        dbname = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        host = os.getenv("DB_HOST")
    )

    return connection

def db_setup(file_name : str = FILE_NAME, reset = False):
    # Basically everything that main does without test queries , so that it can be called in main.py
    data = read(file_name)

    connection = connect_db()
    cur = connection.cursor()

    if reset:
        reset_sequence(cur)

    load_data(cur, data)
    connection.commit()
    verify_load(cur)

    cur.close()
    connection.close()

if __name__ == "__main__":
    # Check the connections and the functions
    db_setup("movies.csv", True)


    
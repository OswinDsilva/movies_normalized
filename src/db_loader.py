from pathlib import Path
from psycopg2.extensions import connection as Connection, cursor as Cursor
from typing import List, Dict, Any
import psycopg2 as psy
import dotenv 
import os

def create_tables(cur : Cursor):
    # Create the different tables and define their schema 
    pass

def load_data(cur : Cursor, data : List[Dict[str,Any]]):
    # Load data into all the tables
    pass

def verify_load(cur : Cursor):
    # Just a few queries to test that data has been loaded
    pass

def connect_db():
    # Connect to the database using .env data
    pass

def db_setup(data : List[Dict[str, Any]]):
    # Basically everything that main does without test queries , so that it can be called in main.py
    pass

if __name__ == "__main__":
    # Check the connections and the functions
    pass
from src.clean import cleaner
from src.db_loader import db_setup
from src.read import reader
from src.queries import test_query

FILE_NAME = "movies.csv"

if __name__ == "__main__":
    data = reader(FILE_NAME)
    cleaned_data = cleaner(data)
    db_setup(cleaned_data)

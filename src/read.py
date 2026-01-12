from pathlib import Path
from typing import Dict, List, Any
import csv

ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"

def reader(file_name : str) -> List[Dict[str, Any]]:
    # Reads the files and returns a List of Dictionary of each row

    path = DATA_DIR / file_name

    data = []

    if not path.exists():
        raise FileNotFoundError(f"{file_name} could not be found in data folder")

    with open(path, 'r', encoding="utf-8") as f:
        csv_data = csv.DictReader(f)
        data = list(csv_data)

    return data
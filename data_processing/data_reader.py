"""
DataReader Class
This class handles reading JSON files and converting them to dataframes.
"""


import pandas as pd
import json
from pathlib import Path

class DataReader:
    def __init__(self, base_dir):
        self.base_dir = Path(base_dir)

    def read_json_lines(self, filename):
        file_path = self.base_dir / 'data' / filename
        data = []
        with open(file_path, 'r') as file:
            for line in file:
                data.append(json.loads(line))
        return data

    def flatten_and_normalize(self, data, record_path=None, meta=None):
        return pd.json_normalize(data, record_path, meta, sep='_')

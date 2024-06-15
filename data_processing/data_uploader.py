"""
DataUploader Class
This class handles uploading dataframes to Postgres.
"""


from sqlalchemy import create_engine, String, Integer, Float, DateTime, Boolean

class DataUploader:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def upload_to_db(self, df, table_name, dtype):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False, dtype=dtype)

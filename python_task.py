import logging

import pandas as pd
from sqlalchemy import create_engine, text

import config
import sql

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataBase:
    def __init__(self, db_host, db_name, db_user, db_pass, db_port):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_port = db_port
        self.conn = None

    def connection(self):
        """Connect to the PostgreSQL database"""
        try:
            self.conn = create_engine(
                f"postgresql+psycopg2://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}"
            )
            self.conn.connect()
            logging.info("Database connection established")
        except Exception as error:
            logging.error(f"Error while connecting to database: {error}")
            raise

    def write_file(
        self,
        table,
        file_path,
    ):
        """Write json file into  PostgreSQL database"""
        data = pd.read_json(file_path)
        try:
            data.to_sql(name=table, con=self.conn, if_exists="replace", index=False)
            logging.info(f"Data inserted into {table} table")
        except Exception as error:
            logging.error(f"Error while inserting data into {table} table: {error}")
            raise

    def select_data(self, query, file_format):
        """Select data from database"""
        try:
            with self.conn.begin() as conn:
                df = pd.read_sql_query(sql=text(query), con=conn)
                if file_format == "json":
                    df.to_json("query_result//query.json")
                    logging.info(
                        f"Query successfully completed, find result in query.{file_format} file"
                    )
                elif file_format == "xml":
                    df.to_xml("query_result//query.xml")
                    logging.info(
                        f"Query successfully completed, find result in query.{file_format} file"
                    )
                else:
                    logging.error(
                        "Error while selecting data from database: no such format"
                    )
                    return Exception
        except Exception as error:
            logging.error(f"Error while selecting data from database: {error}")
            raise

    def add_index(self):
        """Add indexes to students and rooms table"""
        try:
            with self.conn.begin() as conn:
                query = text(
                    "CREATE INDEX IF NOT EXISTS idx_rooms_id_stud ON students (room)"
                )
                conn.execute(query)
                query = text("CREATE INDEX IF NOT EXISTS idx_rooms_id ON rooms (id)")
                conn.execute(query)
            logging.info("Indexes successfully added")
        except Exception as error:
            logging.error(f"Error while creating indexes: {error}")
            raise


db = DataBase("localhost", "python_task", "postgres", "1996", "5432")
db.connection()

db.write_file("rooms", config.rooms_file)
db.write_file("students", config.students_file)

db.select_data(query=sql.rooms_avg_age, file_format="json")
db.add_index()

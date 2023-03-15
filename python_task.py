import pandas as pd
from sqlalchemy import create_engine, engine, text
import logging
import sql

import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataBase:
    def __init__(self,db_host, db_name, db_user, db_pass, db_port):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_port = db_port
        self.conn = None


    def connection(self):
        """ Connect to the PostgreSQL database """
        try:
            self.conn = create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}')
            self.conn.connect()
            logging.info("Database connection established")
        except Exception as error:
            logging.error(f"Error while connecting to database: {error}")
            raise


    def write_file(self, table_name: 'rooms, students', file_path: 'rooms_file, students_file'):
        """ Write json file into  PostgreSQL database """
        data = pd.read_json(file_path)
        try:
            data.to_sql(name=table_name, con=self.conn, if_exists='replace', index=False)
            logging.info(f"Data inserted into {table_name} table")
        except Exception as error:
            logging.error(f"Error while inserting data into {table_name} table: {error}")
            raise


    def select_data(self, query: 'choose query from sql. file', format: 'json, xml'):
        """ Select data from database """
        try:
            with self.conn.begin() as conn:
                df = pd.read_sql_query(sql=text(query), con=conn)
                if format == 'json':
                    df.to_json('query_result//query.json')
                    logging.info(f"Query successfully completed, find result in query.{format} file")
                elif format == 'xml':
                    df.to_xml('query_result//query.xml')
                    logging.info(f"Query successfully completed, find result in query.{format} file")
                else:
                    logging.error(f"Error while selecting data from database: no such format")
                    return False
        except Exception as error:
            logging.error(f"Error while selecting data from database: {error}")
            raise


    def add_index(self):
        """ Add indexes to students and rooms table """
        try:
            with self.conn.begin() as conn:
                query  = text('CREATE INDEX IF NOT EXISTS idx_rooms_id_stud ON students (room)')
                conn.execute(query)
                query = text('CREATE INDEX IF NOT EXISTS idx_rooms_id ON rooms (id)')
                conn.execute(query)
            logging.info(f"Indexes successfully added")
        except Exception as error:
            logging.error(f"Error while creating indexes: {error}")
            raise

db = DataBase('localhost', 'python_task', 'postgres', '1996', '5432')
db.connection()

db.write_file('rooms', config.rooms_file)
db.write_file('students', config.students_file)

db.select_data('SELECT * FROM rooms', format='json')
db.add_index()
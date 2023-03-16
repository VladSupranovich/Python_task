import os

import pandas as pd
import pytest
from sqlalchemy import text

import config
import python_task


@pytest.fixture
def test_db():
    db = python_task.DataBase(
        db_host=config.db_host,
        db_name=config.db_name,
        db_user=config.db_user,
        db_pass=config.db_pass,
        db_port=config.db_port,
    )
    yield db
    db.conn.dispose()


class TestConnection:
    def test_db_connection_successful(self, test_db):
        assert test_db.connection() is None

    def test_db_connection_unsuccessful(self):
        db = python_task.DataBase(
            db_host="localhost",
            db_name="python_task",
            db_user="postgres",
            db_pass="1234",
            db_port="5432",
        )
        with pytest.raises(Exception):
            db.connection()


class TestWriteFile:
    def test_write_file_successful(self, test_db):
        test_db.connection()
        with open("test_file.json", "w", encoding="utf8") as test_file:
            test_file.write(
                '[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]'
            )
        test_db.write_file("test_table", "test_file.json")
        with test_db.conn.begin() as conn:
            query = text("SELECT COUNT(*) FROM test_table")
            assert conn.execute(query).scalar() == 2
        os.remove("test_file.json")


class TestSelectData:
    def test_select_data_json(self, test_db):
        test_db.connection()
        query = "SELECT * FROM test_table"
        file_format = "json"
        test_db.select_data(query, file_format)
        assert os.path.isfile(f"query_result/query.{file_format}")
        data = pd.read_json(f"query_result/query.{file_format}")
        assert len(data) == 2
        assert data["name"][0] == "John"
        assert data["age"][0] == 30

    def test_select_data_xml(self, test_db):
        test_db.connection()
        query = "SELECT * FROM test_table"
        file_format = "xml"
        test_db.select_data(query, file_format)
        assert os.path.isfile(f"query_result/query.{file_format}")
        data = pd.read_xml(f"query_result/query.{file_format}")
        assert len(data) == 2
        assert data["name"][0] == "John"
        assert data["age"][0] == 30

    def test_select_data_format_error(self, test_db):
        test_db.connection()
        query = "SELECT * FROM test_table"
        file_format = "sql"
        assert test_db.select_data(query, file_format) == Exception

    def test_select_data_sql_error(self, test_db):
        test_db.connection()
        query = "SELECT * FROM fake_table"
        file_format = "json"
        with pytest.raises(Exception):
            test_db.select_data(query, file_format)


class TestAddIndex:
    def test_add_index(self, test_db):
        test_db.connection()
        test_db.add_index()
        query = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
        with test_db.conn.begin() as conn:
            df = pd.read_sql_query(sql=text(query), con=conn)
            assert df["indexname"][0] == "idx_rooms_id_stud"
            assert df["indexname"][1] == "idx_rooms_id"

Here you can find information how to use python_task.py script.

Данный сктрип позволяет подключаться к базе данных PostgreSQL, 
загружать данные в таблицы из json файлов, делать SELECT запросы к базе данных и выгружать их в файлы в формате json или xml.

Скрипт содержит класс DataBase, в классе присутствуют 4 функции: connection, write_file, select_data, add_index.

Чтобы создать новое подключение к базе данных, нужно создать объект класса DataBase, с кредами для подключения к базе данных:
db = DataBase(db_host, db_name, db_user, db_pass, db_port)

Далле нижно вызвать функцию connection, чтобы подключиться к базе данных:
db.connection()

Функция write_file загружает данные из файлов в таблицы базы данных, функция принимает два аргумента, имя таблицы и путь к файлу:
db.write_file(table_name, file_path)

Функция select_data делает запрос в базу даннных и сохраняет результат в файл формата json или xlm,
функция принимает два аргумента запрос(все запросы прописаны в файле sql.py) и формат выходного файла.
db.select_data(sql_query, file_format)

Функция add_index добавляет индекс в таблицу для ускорения запросов выборки:
db.add_index()
import sqlite3
import time
import sqlite3
import os


def load_sqlite_data(db_path, sql_file):
    # Remove the existing db file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

    # Connect to SQLite (it'll create a new file)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(sql_file, "r") as f:
        sql_statements = f.read().split(";")
        for statement in sql_statements:
            if statement.strip():
                cursor.execute(statement)

    conn.commit()
    conn.close()

    print(f"Data loaded into {db_path} successfully!")


def check_sqlite_indexes(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'index';"
    )
    indexes = cursor.fetchall()

    print("SQLite Indexes:")
    for index in indexes:
        print(index)

    conn.close()


def calculate_time(db_path):
    # select t1.doc_id from document_keyword as t1
    #  join document_keyword as t2 on t1.doc_id = t2.doc_id
    #  where t1.kw_id = 4125 and t2.kw_id = 47660
    #  order by t1.created_utc;
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    start = time.time()
    cursor.execute(
        "select t1.doc_id from document_keyword as t1 join document_keyword as t2 on t1.doc_id = t2.doc_id where t1.kw_id = 4125 and t2.kw_id = 47660 order by t1.created_utc;"
    )
    end1 = time.time()
    print(f"Time taken to execute the query: {end1 - start:.2f} seconds")
    rows = cursor.fetchall()
    end2 = time.time()
    print(f"Time taken to fetchall: {end2 - end1:.2f} seconds")
    print(f"Total time taken: {end2 - start:.2f} seconds")
    print(f"Number of rows: {len(rows)}")


def main():
    db_path = "keywords.db"
    sql_file = "data.sql"
    # load_sqlite_data(db_path, sql_file)
    # check_sqlite_indexes(db_path)
    calculate_time(db_path)


if __name__ == "__main__":
    main()

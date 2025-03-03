import sqlite3
import time
import os


class SQLiteBenchmark:
    def __init__(
        self,
        db_path="benchmark_sqlite.db",
        sql_file_path="./sql/benchmark_db_sqlite.sql",
    ):
        self.sql_file_path = sql_file_path
        self.db_path = db_path

    def initialize_db(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA cache_size = 33554432;")
            conn.execute("PRAGMA mmap_size = 67108864;")
            conn.execute("DROP TABLE IF EXISTS keywords;")

            with open(self.sql_file_path, "r") as f:
                sql_statements = f.read().split(";")
                for statement in sql_statements:
                    if statement.strip():
                        conn.execute(statement)

            conn.commit()
            print("Database initialized fresh with the table.")

    def replicate_and_upsert(self, keywords, close_after=True):
        db_size_before = os.path.getsize(self.db_path)
        start = time.time()
        with sqlite3.connect(self.db_path) as conn:
            for keyword in keywords:
                conn.execute(
                    """
                    INSERT INTO keywords (keyword, count)
                    VALUES (?, 1)
                    ON CONFLICT(keyword) DO UPDATE SET count = count + 1
                    """,
                    (keyword,),
                )
            conn.commit()
        if close_after:
            conn.close()
        end = time.time()
        print(
            f"Time taken to upsert {len(keywords)} keywords: {end - start:.2f} seconds, Time per keyword: {(end - start) / len(keywords):.6f} seconds"
        )
        db_size_after = os.path.getsize(self.db_path)
        print(
            f"DB Size Before: {db_size_before/1024} KB, DB Size After: {db_size_after/1024} KB"
        )

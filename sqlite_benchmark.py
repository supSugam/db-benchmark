import aiosqlite
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

    async def initialize_db(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA journal_mode=WAL;")

            await conn.execute("DROP TABLE IF EXISTS keywords;")

            with open(self.sql_file_path, "r") as f:
                sql_statements = f.read().split(";")
                for statement in sql_statements:
                    if statement.strip():
                        await conn.execute(statement)

            await conn.commit()
            print("Database initialized fresh with the table.")

    async def replicate_and_upsert(self, keywords):
        async with aiosqlite.connect(self.db_path) as conn:
            start = time.time()
            for keyword in keywords:
                await conn.execute(
                    """
                    INSERT INTO keywords (keyword, count)
                    VALUES (?, 1)
                    ON CONFLICT(keyword) DO UPDATE SET count = count + 1
                    """,
                    (keyword,),
                )
            await conn.commit()
            end = time.time()

            print(
                f"Time taken to upsert {len(keywords)} keywords: {end - start:.2f} seconds, Time per keyword: {(end - start) / len(keywords):.6f} seconds"
            )

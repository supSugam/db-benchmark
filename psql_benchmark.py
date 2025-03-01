import asyncpg
import time


class PostgresBenchmark:
    def __init__(
        self,
        db_name="benchmark_db",
        user="user",
        password="password",
        host="localhost",
        sql_file_path="./sql/benchmark_db_psql.sql",
    ):
        self.sql_file_path = sql_file_path
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    async def initialize_db(self):
        self.conn = await asyncpg.connect(
            user=self.user,
            password=self.password,
            database=self.db_name,
            host=self.host,
        )
        await self.conn.execute("DROP TABLE IF EXISTS keywords;")
        await self.conn.execute("DROP SEQUENCE IF EXISTS keywords_seq;")

        with open(self.sql_file_path, "r") as f:
            sql_statements = f.read().split(";")
            for statement in sql_statements:
                if statement.strip():
                    await self.conn.execute(statement)

        print("Database initialized fresh with the table and sequence.")

    async def replicate_and_upsert(self, keywords):
        # Check DB Size Before
        db_size_before = await self.conn.fetchval(
            "SELECT pg_size_pretty(pg_database_size($1))", self.db_name
        )

        start = time.time()
        for keyword in keywords:
            await self.conn.execute(
                """
                INSERT INTO keywords (keyword) 
                VALUES ($1)
                ON CONFLICT (keyword) 
                DO UPDATE SET count = keywords.count + 1
                """,
                keyword,
            )
        end = time.time()

        print(
            f"Time taken to upsert {len(keywords)} keywords: {end - start:.2f} seconds, Time per keyword: {(end - start) / len(keywords):.4f} seconds"
        )

        # Check DB Size After
        db_size_after = await self.conn.fetchval(
            "SELECT pg_size_pretty(pg_database_size($1))", self.db_name
        )
        print(f"DB Size Before: {db_size_before}, DB Size After: {db_size_after}")

    async def close(self):
        if self.conn:
            await self.conn.close()

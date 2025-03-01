from utils import replicate_and_upsert, generate_keywords
import asyncpg
import asyncio
import argparse

SQL_FILE_PATH = "./sql/benchmark_db.sql"


async def initialize_db(conn: asyncpg.Connection):
    await conn.execute("DROP TABLE IF EXISTS keywords;")
    await conn.execute("DROP SEQUENCE IF EXISTS keywords_seq;")

    with open(SQL_FILE_PATH, "r") as f:
        sql = f.read()
        await conn.execute(sql)
    print("Database initialized fresh with the table and sequence.")


async def main():
    # Args
    parser = argparse.ArgumentParser(
        description="Run a PostgreSQL benchmark for keyword upsert"
    )
    parser.add_argument(
        "--num_keywords", type=int, default=2000, help="Number of unique keywords"
    )
    parser.add_argument(
        "--replicate",
        type=int,
        default=50,
        help="Number of times to replicate each keyword",
    )
    parser.add_argument(
        "--keylength", type=int, default=8, help="Length of the keywords"
    )

    args = parser.parse_args()
    conn = conn = await asyncpg.connect(
        user="user", password="password", database="benchmark_db", host="localhost"
    )
    await initialize_db(conn)

    keywords = generate_keywords(args.keylength, args.num_keywords, args.replicate)
    await replicate_and_upsert(conn, keywords)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

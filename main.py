from utils import generate_keywords
import asyncio
import argparse
from psql_benchmark import PostgresBenchmark
from sqlite_benchmark import SQLiteBenchmark


async def main():
    # Args
    parser = argparse.ArgumentParser(description="Performance Test for keyword upsert")
    parser.add_argument(
        "--database",
        type=str,
        default="postgres",
        help="Database to connect to (postgres/sqlite, default: postgres)",
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

    # Error if --database is not postgres or sqlite
    if args.database not in ["postgres", "sqlite"]:
        raise ValueError(f"Database {args.database} not implemented")

    keywords = generate_keywords(args.keylength, args.num_keywords, args.replicate)
    if args.database == "postgres":
        psqldb = PostgresBenchmark()
        await psqldb.initialize_db()
        await psqldb.replicate_and_upsert(keywords)
        await psqldb.close()
    elif args.database == "sqlite":
        sqlitedb = SQLiteBenchmark()
        sqlitedb.initialize_db()
        sqlitedb.replicate_and_upsert(keywords)


if __name__ == "__main__":
    asyncio.run(main())

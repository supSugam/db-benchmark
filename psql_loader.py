import asyncpg
import asyncio
import time
import os


async def load_postgres_data(sql_file):
    # Connect to PostgreSQL asynchronously
    conn = await asyncpg.connect(
        user="user",
        password="password",
        database="keywords_db",
        host="localhost",
    )

    # Read the SQL file
    with open(sql_file, "r") as f:
        sql_data = f.read()

    # Execute the SQL to create tables and load data
    await conn.execute(sql_data)

    print("Data loaded into PostgreSQL successfully!")

    # Close connection
    await conn.close()


async def check_postgres_indexes():
    # Connect to PostgreSQL asynchronously
    conn = await asyncpg.connect(
        user="user",
        password="password",
        database="keywords_db",
        host="localhost",
    )

    # Fetch the list of indexes from PostgreSQL
    indexes = await conn.fetch(
        "SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public';"
    )

    print("PostgreSQL Indexes:")
    for index in indexes:
        print(index)

    await conn.close()


async def calculate_time():
    # Connect to PostgreSQL asynchronously
    conn = await asyncpg.connect(
        user="user",
        password="password",
        database="keywords_db",
        host="localhost",
    )

    start = time.time()

    # Example query to calculate time
    query = """
        SELECT t1.doc_id 
        FROM document_keyword AS t1
        JOIN document_keyword AS t2 ON t1.doc_id = t2.doc_id
        WHERE t1.kw_id = 4125 AND t2.kw_id = 47660
        ORDER BY t1.created_utc;
    """
    rows = await conn.fetch(query)

    end1 = time.time()
    print(f"Time taken to execute the query: {end1 - start:.2f} seconds")
    end2 = time.time()
    print(f"Time taken to fetchall: {end2 - end1:.2f} seconds")
    print(f"Total time taken: {end2 - start:.2f} seconds")
    print(f"Number of rows: {len(rows)}")

    await conn.close()


async def calculate_db_size():
    # Connect to PostgreSQL asynchronously
    conn = await asyncpg.connect(
        user="user",
        password="password",
        database="keywords_db",
        host="localhost",
    )

    # Calculate the size of the database
    query = """
        SELECT pg_size_pretty(pg_database_size('keywords_db'));
    """
    db_size = await conn.fetchval(query)

    print(f"Database size: {db_size}")

    await conn.close()


async def main():

    # Check the indexes
    await check_postgres_indexes()

    # Calculate query time
    await calculate_time()

    # Calculate database size
    await calculate_db_size()


# Run everything
asyncio.run(main())

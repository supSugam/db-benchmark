import random
import string
import asyncpg
import time


def generate_random_string(length):
    return "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(length)
    )


def generate_keywords(key_length, num_keywords=2000, replicate=50):
    keywords = [generate_random_string(key_length) for _ in range(num_keywords)]
    keywords_replicated = keywords * replicate
    random.shuffle(keywords_replicated)

    return keywords_replicated


async def replicate_and_upsert(conn: asyncpg.Connection, keywords):
    # Check DB Size
    db_size_before = await conn.fetchval(
        "SELECT pg_size_pretty(pg_database_size('benchmark_db'))"
    )

    start = time.time()
    for keyword in keywords:
        await conn.execute(
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
        f"Time taken to upsert {len(keywords)} keywords: {end - start:.2f} seconds, Time taken per keyword: {(end - start) / len(keywords):.4f} seconds"
    )
    db_size_after = await conn.fetchval(
        "SELECT pg_size_pretty(pg_database_size('benchmark_db'))"
    )
    print(f"DB Size Before: {db_size_before}, DB Size After: {db_size_after}")

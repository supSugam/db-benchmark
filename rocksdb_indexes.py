import os
import time
import re
import concurrent.futures
import struct
from rocksdict import Rdict, Options

# Directory for databases
BASE_DIR = "./document_keyword_rocksdbs"
os.system(f"rm -rf {BASE_DIR}")
os.makedirs(BASE_DIR, exist_ok=True)

# Create separate directories for each index
DB_DIRS = {
    "idx_seq_date": f"{BASE_DIR}/idx_seq_date",
    "idx_seq_auth_date": f"{BASE_DIR}/idx_seq_auth_date",
    "idx_seq_score": f"{BASE_DIR}/idx_seq_score",
    "idx_seq_auth_score": f"{BASE_DIR}/idx_seq_auth_score",
}

# Create directories
for dir_path in DB_DIRS.values():
    os.makedirs(dir_path, exist_ok=True)

# Open RocksDB environments
options = Options()
envs = {
    "idx_seq_date": Rdict(DB_DIRS["idx_seq_date"], options),
    "idx_seq_auth_date": Rdict(DB_DIRS["idx_seq_auth_date"], options),
    "idx_seq_score": Rdict(DB_DIRS["idx_seq_score"], options),
    "idx_seq_auth_score": Rdict(DB_DIRS["idx_seq_auth_score"], options),
}


def encode_key(elements):
    """Encode elements into binary key in order."""
    result = b""
    for elem in elements:
        if isinstance(elem, int):
            result += struct.pack(">q", elem)  # 8-byte big-endian integer
        elif isinstance(elem, str):
            bytes_data = elem.encode("utf-8")
            result += struct.pack(">I", len(bytes_data))  # 4-byte length prefix
            result += bytes_data
    return result


def process_batch(db_name, batch):
    """Process a batch of records for a specific database."""
    env = envs[db_name]
    for kw_id, doc_id, subreddit, author, created_utc, score in batch:
        doc_id_bytes = struct.pack(">q", doc_id)

        if db_name == "idx_seq_date":
            key = encode_key([kw_id, created_utc])
        elif db_name == "idx_seq_auth_date":
            key = encode_key([kw_id, author, created_utc])
        elif db_name == "idx_seq_score":
            key = encode_key([kw_id, score])
        elif db_name == "idx_seq_auth_score":
            key = encode_key([kw_id, author, score])

        env.put(key, doc_id_bytes)

    return f"Processed batch for {db_name}"


def extract_values_from_sql(file_path):
    start = time.time()
    values_list = []
    pattern = re.compile(r"INSERT INTO \w+ VALUES\((.*?)\);")
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            match = pattern.search(line)
            if match:
                values = match.group(1).split(",")
                values_tuple = tuple(
                    int(x) if x.strip().isdigit() else x.strip("'") for x in values
                )
                values_list.append(values_tuple)
    print(f"Time taken to extract values from SQL: {time.time()-start}")
    return values_list


def create_tasks(values_list, batch_size=1000):
    # Create batches
    batches = [
        values_list[i : i + batch_size] for i in range(0, len(values_list), batch_size)
    ]

    # Create a list of tasks (db_name, batch)
    tasks = []
    for db_name in envs.keys():
        for batch in batches:
            tasks.append((db_name, batch))

    return tasks


# Main execution
values_list = extract_values_from_sql("data/data.sql")

store_start = time.time()

max_workers = min(8, os.cpu_count() - 4)
print(f"Using {max_workers} worker threads")

# Create tasks and run them in parallel
tasks = create_tasks(values_list)
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [
        executor.submit(process_batch, db_name, batch) for db_name, batch in tasks
    ]

    for future in concurrent.futures.as_completed(futures):
        try:
            result = future.result()
            print(result)
        except Exception as e:
            print(f"Error: {e}")

print(f"Time taken to store all documents: {time.time()-store_start}")

# Close all environments
for env in envs.values():
    env.close()

import snowflake.connector
import os
import math
import more_itertools
from multiprocessing.pool import Pool
from dotenv import load_dotenv
import requests

load_dotenv()

conn = snowflake.connector.connect(
        user = os.getenv('SF_USER'), # USER and USERNAME are already used
        password = os.getenv('PASSWORD'),
        account = os.getenv('ACCOUNT'),
        warehouse = os.getenv('WAREHOUSE'),
        database = os.getenv('DATABASE'),
        schema = os.getenv('SCHEMA')
        )
cur = conn.cursor()

try:
    (max_db_id,) = cur.execute('select max(id) from items;').fetchone()
except snowflake.connector.errors.ProgrammingError as e:
    cur.close()
    conn.close()
    raise e

latest_id = requests.get('https://hacker-news.firebaseio.com/v0/maxitem.json').json()

num_processes = os.cpu_count() * 4

ids_to_fetch = range(max_db_id + 1, latest_id + 1)

print(f'fetching {len(ids_to_fetch)} items')

try:
    os.mkdir('data')
except FileExistsError as e:
    pass

ids_per_process = math.ceil(len(ids_to_fetch) / num_processes)

split_ids_to_fetch = more_itertools.divide(num_processes, ids_to_fetch)

def job(ids):
    pid = os.getpid()

    items = []
    for id_ in ids:
        url = f'https://hacker-news.firebaseio.com/v0/item/{id_}.json'
        item = requests.get(url).text
        items.append(item)

    filename = f'data/items_{pid}'
    with open(filename, 'w') as f:
        content = '\n'.join(items)
        f.write(content + '\n')

    return filename

with Pool(processes = num_processes) as pool:
    print(pool.map(job, split_ids_to_fetch))

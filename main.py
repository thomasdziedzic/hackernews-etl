import snowflake.connector
import os
import time
import glob
import math
import more_itertools
import subprocess
from datetime import datetime
import multiprocessing
from multiprocessing.pool import Pool
from dotenv import load_dotenv
import requests
from atpbar import atpbar, register_reporter, find_reporter, flush

multiprocessing.set_start_method('fork', force=True)

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
    # fetch items 1 week before the latest item we have to account for any updates that may happen to the items
    (max_db_id,) = cur.execute("""
        select max(id)
        from items
        where time < (select max(time) - interval '1 week' from items);
    """).fetchone()
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

# cleanup data directory from previous runs
for f in glob.glob('./data/*'):
    os.remove(f)

ids_per_process = math.ceil(len(ids_to_fetch) / num_processes)

split_ids_to_fetch = more_itertools.divide(num_processes, ids_to_fetch)

rate_limit = 300 # in requests / second
rate_limit_per_job = rate_limit / num_processes
seconds_to_wait_between_requests = 1 / rate_limit_per_job

def job(ids):
    pid = os.getpid()

    filename = f'data/items_{pid}'
    with open(filename, 'w') as f:
        for id_ in atpbar(list(ids), name = multiprocessing.current_process().name):
            start_time = time.time()

            url = f'https://hacker-news.firebaseio.com/v0/item/{id_}.json'
            item = requests.get(url).text
            f.write(item + '\n')

            end_time = time.time()
            elapsed_time = end_time - start_time

            if elapsed_time < seconds_to_wait_between_requests:
                time.sleep(seconds_to_wait_between_requests - elapsed_time)


    return filename

reporter = find_reporter()

with Pool(processes = num_processes, initializer = register_reporter, initargs = [reporter]) as pool:
    item_part_files = pool.map(job, split_ids_to_fetch)
    flush()

formatted_date = datetime.utcnow().date().strftime("%Y_%m_%d")
subprocess.run(f'cat data/* > data/all_items_{formatted_date}.json', shell = True, check = True)

all_items_full_path = f'{os.getcwd()}/data/all_items_{formatted_date}.json'

try:
    # cleanup any previously staged files
    cur.execute('remove @%raw_items;')

    # the table stage is an implicit stage created for every table so no need to create it
    # snowflake put will auto_compress by default into gz
    cur.execute(f'put file://{all_items_full_path} @%raw_items;')

    cur.execute('truncate raw_items;')

    cur.execute(f"""
        copy into raw_items
        from @%raw_items/all_items_{formatted_date}.json.gz
        file_format = ( type = json );
    """)

    cur.execute(f"""
        merge into items as trg using raw_items as src on trg.id = src.item:id
          when matched and trg.id is null then delete
          when matched then update set
            trg.id = src.item:id,
            trg.deleted = src.item:deleted,
            trg.type = src.item:type,
            trg.by_ = src.item:by_,
            trg.time = src.item:time,
            trg.text = src.item:text,
            trg.dead = src.item:dead,
            trg.parent = src.item:parent,
            trg.poll = src.item:poll,
            trg.kids = src.item:kids,
            trg.url = src.item:url,
            trg.score = src.item:score,
            trg.title = src.item:title,
            trg.parts = src.item:parts,
            trg.descendants = src.item:descendants
          when not matched and src.item:id is not null then insert (
            id,
            deleted,
            type,
            by_,
            time,
            text,
            dead,
            parent,
            poll,
            kids,
            url,
            score,
            title,
            parts,
            descendants
          ) values (
            src.item:id,
            src.item:deleted,
            src.item:type,
            src.item:by,
            src.item:time,
            src.item:text,
            src.item:dead,
            src.item:parent,
            src.item:poll,
            src.item:kids,
            src.item:url,
            src.item:score,
            src.item:title,
            src.item:parts,
            src.item:descendants
          );
    """)

    # cleanup after we're done
    cur.execute('remove @%raw_items;')
except snowflake.connector.errors.ProgrammingError as e:
    cur.close()
    conn.close()
    raise e

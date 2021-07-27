import psycopg2
import time
import pandas as pd
import sqlite3

def main():
    # load session
    conn = psycopg2.connect(dbname="SSB", user="postgres", password="password", host="127.0.0.1", port=5433)  # TODO edit here
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # db to store results (this one is just file created on startup)
    sqlite_db = sqlite3.connect("benchmark_data.db")

    # set constants
    n_reps = 2  # TODO change here
    path = "C:\\Program Files\\PostgreSQL\\13\\data\\sgd"  # TODO change here
    tables = ["date", "supplier", "customer", "part", "lineorder"]
    buffer_size = 1024 ** 3  # in bytes # TODO change here
    load_build_time_file = "load_time"
    query_times_file = "queries_time"
    key_times_file = "keys_time"

    # set queries
    with open("clean queries.sql", "r") as queries_file:
        text = queries_file.read()
        queries = text.split(";")
        if queries[-1].strip() == '':
            queries.pop(-1)
        queries = [query.strip() for query in queries]

    # set keys
    with open("ssb.ri", "r") as keys_file:
        text = keys_file.read()
        keys = text.split(";")
        if keys[-1].strip() == '':
            keys.pop(-1)
        keys = [key.strip() for key in keys]

    # repeat process n_reps times
    for rep in range(n_reps):
        # load dictionaries for value storage
        operation_times = {t: list() for t in tables}
        query_times = {
            "query_id": list(),
            "time": list(),
            "has_keys": list()
        }
        key_times = {
            "key_id": list(),
            "time": list()
        }

        print("\n\nRepetition #%d" % rep)
        # clear all tables
        cur.execute(open("reset_db.sql", "r").read())

        load_tables_total = 0
        for table in tables:
            file_path = "%s/%s.tbl" % (path, table)  # TODO EDIT SEPARATOR HERE IF NEEDED
            file = open(file_path, "r")
            start = time.time()
            cur.copy_expert("copy %s from '%s' delimiter ',' csv;" % (table, file_path), file, size=buffer_size)
            took = time.time() - start
            print("Loaded table %s in %ss" % (table, took))
            operation_times[table].append(took)
            load_tables_total += took
        print("Loaded all tables in %s" % load_tables_total)

        # execute queries without built keys
        start_querying = time.time()
        for i, query in enumerate(queries):
            cur = conn.cursor()
            start = time.time()
            cur.execute(query)
            cur.fetchall()
            took = time.time() - start
            query_times["query_id"].append(i)
            query_times["time"].append(took)
            query_times["has_keys"].append(0)
        took_querying = time.time() - start_querying
        print("Performed all queries without keys in %ss" % took_querying)

        # build keys
        start_building = time.time()
        for i, key in enumerate(keys):
            cur = conn.cursor()
            start = time.time()
            cur.execute(key)
            took = time.time() - start
            key_times["key_id"].append(i)
            key_times["time"].append(took)
        took_building = time.time() - start_building
        print("Built all keys in %ss" % took_building)

        # analyse to improve DB usage of keys
        cur.execute("analyze")

        # execute queries with built keys
        start_querying = time.time()
        for i, query in enumerate(queries):
            cur = conn.cursor()
            start = time.time()
            cur.execute(query)
            cur.fetchall()
            took = time.time() - start
            query_times["query_id"].append(i)
            query_times["time"].append(took)
            query_times["has_keys"].append(1)
        took_querying = time.time() - start_querying
        print("Performed all queries with keys in %ss" % took_querying)

        # save experiment results to sqlite file
        df = pd.DataFrame(operation_times)
        df.to_sql(load_build_time_file, sqlite_db, if_exists='append', index=False)

        df = pd.DataFrame(query_times)
        df.to_sql(query_times_file, sqlite_db, if_exists='append', index=False)

        df = pd.DataFrame(key_times)
        df.to_sql(key_times_file, sqlite_db, if_exists='append', index=False)

    # terminate
    sqlite_db.close()
    conn.close()

if __name__ == '__main__':
    main()

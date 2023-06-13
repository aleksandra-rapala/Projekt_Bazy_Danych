import psycopg2
import time
from pymongo import MongoClient
from baza_sql import connect_sql, disconnect_sql
from baza_cassandra import connect_cassandra, disconnect_cassandra
from baza_mongo import connect_mongo, disconnect_mongo
from cassandra.query import SimpleStatement
import numpy as np

def execute_query(cur_pg, query):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    cur_pg.execute(query)
    rows_pg = cur_pg.fetchall()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_pg, execution_time

def calculate_statistics(cur_pg):
    numeric_columns = ['max_rate', 'total_bet_amount', 'total_win_amount', 'total_profit_amount', 'player_count', 'bet_count']
    
    for column in numeric_columns:
        # Obliczenie średniej
        query = f"SELECT AVG({column}) FROM games"
        result, execution_time = execute_query(cur_pg, query)
        avg = result[0][0]
        print(f"Średnia {column}: {avg} [POSTGRESQL]")
        print(f"Czas wykonania: {execution_time} s")
        
        # Obliczenie mediany
        query = f"SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY {column}) FROM games"
        result, execution_time = execute_query(cur_pg, query)
        median = result[0][0]
        print(f"Mediana {column}: {median} [POSTGRESQL]")
        print(f"Czas wykonania: {execution_time} s")
        
        # Obliczenie odchylenia standardowego
        query = f"SELECT STDDEV({column}) FROM games"
        result, execution_time = execute_query(cur_pg, query)
        stddev = result[0][0]
        print(f"Odchylenie standardowe {column}: {stddev}  [POSTGRESQL]")
        print(f"Czas wykonania: {execution_time} s")
        
        print("-----------------------------------")

# Połączenie z bazą danych
cur_pg, conn_pg = connect_sql()

# Wyliczenie statystyk
calculate_statistics(cur_pg)

# Zamknięcie połączenia z bazą danych
disconnect_sql(cur_pg, conn_pg)


############################################ MONGODB ##########################################

def connect_mongo():
    # Połączenie z bazą danych MongoDB
    client_mongo = MongoClient("mongodb://localhost:27017/")
    db_mongo = client_mongo["Projekt2"]
    return db_mongo

def calculate_statistics(db):
    games_collection = db['games']
    numeric_fields = ['max_rate', 'total_bet_amount', 'total_win_amount', 'total_profit_amount', 'player_count', 'bet_count']

    for field in numeric_fields:
        # Obliczenie średniej
        start_time = time.time()
        average_result = games_collection.aggregate([{"$group": {"_id": None, "avg": {"$avg": f"${field}"}}}])
        average = list(average_result)[0]['avg']
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Średnia {field}: {average} [MONGODB]")
        print(f"Czas wykonania: {execution_time} s")

        # Obliczenie mediany
        start_time = time.time()
        sorted_result = games_collection.find().sort(field, 1)
        sorted_list = [doc[field] for doc in sorted_result]
        median = sorted_list[len(sorted_list) // 2] if len(sorted_list) % 2 != 0 else (sorted_list[len(sorted_list) // 2 - 1] + sorted_list[len(sorted_list) // 2]) / 2
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Mediana {field}: {median} [MONGODB]")
        print(f"Czas wykonania: {execution_time} s")

        # Obliczenie odchylenia standardowego
        start_time = time.time()
        stddev_result = games_collection.aggregate([{"$group": {"_id": None, "stddev": {"$stdDevSamp": f"${field}"}}}])
        stddev = list(stddev_result)[0]['stddev']
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Odchylenie standardowe {field}: {stddev} [MONGODB]")
        print(f"Czas wykonania: {execution_time} s")

        print("-----------------------------------")

# Połączenie z bazą danych MongoDB
db = connect_mongo()

# Wyliczenie statystyk
calculate_statistics(db)

# Zamknięcie połączenia z bazą danych MongoDB
#disconnect_mongo(db)



################################################################ CASSANDRA #####################################

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def calculate_median(session, column):
    query = f"SELECT {column} FROM games"
    rows = session.execute(query)

    values = [row[0] for row in rows if row]
    values.sort()

    count = len(values)
    middle = count // 2

    if count % 2 == 0:
        median = (values[middle - 1] + values[middle]) / 2
    else:
        median = values[middle]

    return median

def calculate_standard_deviation(session, column):
    query = f"SELECT {column} FROM games"
    rows = session.execute(query)

    values = [row[0] for row in rows if row]
    mean = np.mean(values)
    deviations = [(x - mean) ** 2 for x in values]
    variance = np.mean(deviations)
    standard_deviation = np.sqrt(variance)

    return standard_deviation





def calculate_statistics(session):
    numeric_fields = ['max_rate', 'total_bet_amount', 'total_win_amount', 'total_profit_amount', 'player_count', 'bet_count']

    for field in numeric_fields:
        # Obliczenie średniej
        start_time = time.time()  # Początkowy czas wykonania zapytania
        average_result = session.execute(f"SELECT AVG({field}) FROM games")
        average = average_result.one()[0]
        end_time = time.time()  # Końcowy czas wykonania zapytania
        execution_time = end_time - start_time  # Czas wykonania zapytania
        print(f"Średnia {field}: {average} [CASSANDRA]" )
        print(f"Czas wykonania: {execution_time} s")

        # Obliczenie mediany
        start_time = time.time()  # Początkowy czas wykonania zapytania
        median = calculate_median(session, field)
        end_time = time.time()  # Końcowy czas wykonania zapytania
        execution_time = end_time - start_time  # Czas wykonania zapytania
        print(f"Mediana {field}: {median} [CASSANDRA]")
        print(f"Czas wykonania: {execution_time} s")

        # Obliczenie odchylenia standardowego
        start_time = time.time()  # Początkowy czas wykonania zapytania
        stddev = calculate_standard_deviation(session, field)
        end_time = time.time()  # Końcowy czas wykonania zapytania
        execution_time = end_time - start_time  # Czas wykonania zapytania
        print(f"Odchylenie standardowe {field}: {stddev} [CASSANDRA]")
        print(f"Czas wykonania: {execution_time} s")
        print("-----------------------------------")

# Połączenie z bazą danych CassandraDB
session = connect_cassandra()

# Wyliczenie statystyk
calculate_statistics(session)

# Zamknięcie połączenia z bazą danych CassandraDB
disconnect_cassandra(session)
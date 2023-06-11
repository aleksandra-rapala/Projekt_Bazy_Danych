
from pymongo import MongoClient
import time

def connect_mongo():
    # Połączenie z bazą danych MongoDB
    client_mongo = MongoClient("mongodb://localhost:27017/")
    db_mongo = client_mongo["Projekt2"]
    return db_mongo, client_mongo

def disconnect_mongo(client_mongo):
    # Zamknięcie połączenia z bazą danych MongoDB
    client_mongo.close()

def mongo_select(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania find
    results = db_mongo.game_types.find({'name_game_type': "yellow"})
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return results, execution_time

def mongo_insert(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania insert
    document = {
    "id_game_type": 555,
    "name_game_type": "orange"
    }
    db_mongo.game_types.insert_one(document)
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

def mongo_update(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania update
    db_mongo.game_types.update_one({"name_game_type": "orange"}, {"$set": {"name_game_type": "pink"}})
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

def mongo_delete(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania delete
    document = {
    "id_game_type": 555,
    "name_game_type": "orange"
    }
    db_mongo.game_types.delete_one(document)
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

import pymongo

def connect_mongo():
    # Połączenie z bazą danych MongoDB
    client_mongo = pymongo.MongoClient("mongodb://localhost:27017/")
    db_mongo = client_mongo["nazwa"]
    col_mongo = db_mongo["nazwa_tabeli"]
    return client_mongo, col_mongo
   
def disconnect_mongo(client_mongo):
    # Zamknięcie połączenia z bazą danych MongoDB
    client_mongo.close()

def mongo_select(col_mongo):
    # Utworzenie dokumentu
    doc = {"ID": 1, "NAME": "John"}
    # doc2 = {"ID": 2, "NAME": "Matt"}
    x = col_mongo.insert_one(doc)
    # y = col_mongo.insert_one(doc2)

    # Wykonanie polecenia SELECT
    results_mongo = col_mongo.find()
    for result in results_mongo:
        print(result)

# def mongo_delete(cur_pg):

# def mongo_update(cur_pg):

# def mongo_insert(cur_pg):

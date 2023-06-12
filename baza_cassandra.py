from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

def connect_cassandra():
    # Połączenie z bazą danych Cassandra
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['localhost'], auth_provider=auth_provider)
    session = cluster.connect('projektZTBD')
    return session

def disconnect_cassandra(session):
    # Zamknięcie połączenia z bazą danych Cassandra
    session.shutdown()

def cassandra_select(session):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania SELECT
    rows = session.execute('SELECT * FROM game_types')
    rows_cassandra = list(rows)
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_cassandra, execution_time

def cassandra_delete(session):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania DELETE
    session.execute("DELETE FROM game_types WHERE id_game_type = 555")
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

def cassandra_update(session):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania UPDATE
    session.execute("UPDATE game_types SET name_game_type = 'blue' WHERE id_game_type = 555")
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

def cassandra_insert(session):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania INSERT
    session.execute("INSERT INTO game_types (id_game_type, name_game_type) VALUES (555, 'orange')")
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time
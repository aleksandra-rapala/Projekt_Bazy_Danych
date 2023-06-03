import psycopg2
import time

def connect_sql():
    # Połączenie z bazą danych PostgreSQL
    conn_pg = psycopg2.connect(database="Projekt2", user="postgres", password="rootola", host="localhost", port="5432")
    cur_pg = conn_pg.cursor()
    return cur_pg, conn_pg
   
def disconnect_sql(cur_pg, conn_pg):
    # Zamknięcie połączenia z bazą danych PostgreSQL
    cur_pg.close()
    conn_pg.close()

def sql_select(cur_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie polecenia SELECT
    cur_pg.execute('''SELECT * from game_types''')
    rows_pg = cur_pg.fetchall()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_pg, execution_time

def sql_delete(cur_pg, conn_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    cur_pg.execute('''DELETE FROM game_types WHERE name_game_type='orange';''');
    conn_pg.commit()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

def sql_update(cur_pg, conn_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    cur_pg.execute('''UPDATE game_types SET name_game_type = REPLACE(name_game_type, 'blu', 'blue');''');
    conn_pg.commit()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

def sql_insert(cur_pg, conn_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    cur_pg.execute('''INSERT INTO game_types (id_game_type, name_game_type) VALUES (555, 'orange');''');
    conn_pg.commit()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return execution_time

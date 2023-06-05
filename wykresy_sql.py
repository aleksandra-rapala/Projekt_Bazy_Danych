import psycopg2
import matplotlib.pyplot as plt
from baza_sql import connect_sql, disconnect_sql, sql_select

# Połącz z bazą danych
cur_pg, conn_pg = connect_sql()

# Wykonaj zapytanie SELECT
rows_pg, execution_time = sql_select(cur_pg)

# Rozłącz z bazą danych
disconnect_sql(cur_pg, conn_pg)

# Wypisz czas zapytania
print("Czas wykonania zapytania SELECT:", execution_time, "sekundy")

# Przetwórz wyniki zapytania
game_types = []
counts = []
for row in rows_pg:
    game_type = row[1]
    count = row[1]
    game_types.append(game_type)
    counts.append(count)

# Wygeneruj wykres słupkowy
plt.bar(game_types, counts)
plt.xlabel('Typ gry')
plt.ylabel('Liczba')
plt.title('Liczba gier dla poszczególnych typów')
plt.show()
import psycopg2
import matplotlib.pyplot as plt
from baza_sql import connect_sql, disconnect_sql, sql_select
import time

def sql_select(cur_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie polecenia SELECT
    cur_pg.execute('''SELECT date, COUNT(*) FROM games GROUP BY date''')
    rows_pg = cur_pg.fetchall()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_pg, execution_time




def generate_game_count_plot(data):
    dates = [row[0] for row in data]
    game_counts = [row[1] for row in data]

    plt.bar(dates, game_counts)
    plt.xlabel('Date')
    plt.ylabel('Game Count')
    plt.title('Game Count by Date')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main():
    # Połączenie z bazą danych SQL
    cur_pg, conn_pg = connect_sql()
    rows_pg, execution_time_sql = sql_select(cur_pg)
    disconnect_sql(cur_pg, conn_pg)


    generate_game_count_plot(rows_pg)

    print("Execution Time (SQL):", execution_time_sql, "seconds")

if __name__ == '__main__':
    main()
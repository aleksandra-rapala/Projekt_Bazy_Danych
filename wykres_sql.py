import psycopg2
import matplotlib.pyplot as plt
from baza_sql import connect_sql, disconnect_sql
import time

def postgresql_select(cur_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie polecenia SELECT w PostgreSQL
    cur_pg.execute('''SELECT date, COUNT(*) FROM games GROUP BY date ORDER BY date''')
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
    plt.title('Game Count by Date (PostgreSQL)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main():
    cur_pg, conn_pg = connect_sql()
    rows_pg, execution_time = postgresql_select(cur_pg)
    disconnect_sql(cur_pg, conn_pg)

    generate_game_count_plot(rows_pg)

    print("Execution Time:", execution_time, "seconds")

def postgresql_select2(cur_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie polecenia SELECT w PostgreSQL
    cur_pg.execute('''SELECT date, SUM(total_win_amount) FROM games GROUP BY date ORDER BY date''')
    rows_pg = cur_pg.fetchall()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_pg, execution_time

def generate_total_win_amount_plot(data):
    dates = [row[0] for row in data]
    total_win_amounts = [row[1] for row in data]

    plt.plot(dates, total_win_amounts)
    plt.xlabel('Date')
    plt.ylabel('Total Win Amount')
    plt.title('Total Win Amount by Date (PostgreSQL)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main2():
    cur_pg, conn_pg = connect_sql()
    rows_pg, execution_time = postgresql_select2(cur_pg)
    disconnect_sql(cur_pg, conn_pg)

    generate_total_win_amount_plot(rows_pg)

    print("Execution Time:", execution_time, "seconds")

def postgresql_select3(cur_pg):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie polecenia SELECT w PostgreSQL
    cur_pg.execute('''SELECT player_count, AVG(total_win_amount) FROM games GROUP BY player_count ORDER BY player_count''')
    rows_pg = cur_pg.fetchall()
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_pg, execution_time

def generate_avg_win_amount_plot(data):
    player_counts = [row[0] for row in data]
    avg_win_amounts = [row[1] for row in data]

    plt.bar(player_counts, avg_win_amounts)
    plt.xlabel('Player Count')
    plt.ylabel('Average Win Amount')
    plt.title('Average Win Amount by Player Count (PostgreSQL)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main3():
    cur_pg, conn_pg = connect_sql()
    rows_pg, execution_time = postgresql_select3(cur_pg)
    disconnect_sql(cur_pg, conn_pg)

    generate_avg_win_amount_plot(rows_pg)

    print("Execution Time:", execution_time, "seconds")

if __name__ == '__main__':
    main()
    main2()
    main3()

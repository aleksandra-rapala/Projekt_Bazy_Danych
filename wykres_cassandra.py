import psycopg2
import matplotlib.pyplot as plt
from baza_cassandra import connect_cassandra, disconnect_cassandra, cassandra_select
import time



def cassandra_select(session):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie polecenia SELECT w bazie Cassandra
    rows = session.execute('SELECT date, COUNT(*) FROM games GROUP BY date')
    results = [(row.date, row.count) for row in rows]
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return results, execution_time


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

    # Połączenie z bazą danych Cassandra
    session_cassandra = connect_cassandra()
    rows_cassandra, execution_time_cassandra = cassandra_select(session_cassandra)
    disconnect_cassandra(session_cassandra)

    generate_game_count_plot(rows_cassandra)

    print("Execution Time (Cassandra):", execution_time_cassandra, "seconds")

if __name__ == '__main__':
    main()
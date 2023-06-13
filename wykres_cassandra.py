import matplotlib.pyplot as plt
import time
from baza_cassandra import connect_cassandra, disconnect_cassandra
from datetime import datetime

def cassandra_select(session):
    start_time = time.time()
    rows = session.execute('''SELECT * FROM games''')
    rows_cassandra = list(rows)
    end_time = time.time()
    execution_time = end_time - start_time
    return rows_cassandra, execution_time

def generate_game_count_plot(data):
    date_count_map = {}
    for row in data:
        date = row.date.date().strftime('%Y-%m-%d')
        if date in date_count_map:
            date_count_map[date] += 1
        else:
            date_count_map[date] = 1

    dates = list(date_count_map.keys())
    game_counts = list(date_count_map.values())

    fig, ax = plt.subplots()
    ax.bar(dates, game_counts)

    plt.xlabel('Date')
    plt.ylabel('Game Count')
    plt.title('Game Count by Date (CassandraDB)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main():
    session = connect_cassandra()
    rows_cassandra, execution_time = cassandra_select(session)
    disconnect_cassandra(session)

    generate_game_count_plot(rows_cassandra)

    print("Execution Time:", execution_time, "seconds")

def cassandra_select2(session):
    start_time = time.time()
    rows = session.execute('''SELECT * FROM games''')
    rows_cassandra = list(rows)
    end_time = time.time()
    execution_time = end_time - start_time
    return rows_cassandra, execution_time

def generate_total_win_amount_plot(data):
    date_amount_map = {}
    for row in data:
        date = row.date.date().strftime('%Y-%m-%d')
        if date in date_amount_map:
            date_amount_map[date] += row.total_win_amount
        else:
            date_amount_map[date] = row.total_win_amount

    dates = list(date_amount_map.keys())
    total_win_amounts = list(date_amount_map.values())

    plt.plot(dates, total_win_amounts)
    plt.xlabel('Date')
    plt.ylabel('Total Win Amount')
    plt.title('Total Win Amount by Date (CassandraDB)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main2():
    session = connect_cassandra()
    rows_cassandra, execution_time = cassandra_select2(session)
    disconnect_cassandra(session)

    generate_total_win_amount_plot(rows_cassandra)

    print("Execution Time:", execution_time, "seconds")

def cassandra_select3(session):
    start_time = time.time()
    rows = session.execute('''SELECT * FROM games''')
    rows_cassandra = list(rows)
    end_time = time.time()
    execution_time = end_time - start_time
    return rows_cassandra, execution_time

def generate_avg_win_amount_plot(data):
    player_count_amount_map = {}
    for row in data:
        player_count = row.player_count
        if player_count in player_count_amount_map:
            player_count_amount_map[player_count][0] += row.total_win_amount
            player_count_amount_map[player_count][1] += 1
        else:
            player_count_amount_map[player_count] = [row.total_win_amount, 1]

    player_counts = []
    avg_win_amounts = []
    for player_count, (total_win_amount, count) in player_count_amount_map.items():
        player_counts.append(player_count)
        avg_win_amounts.append(total_win_amount / count)

    plt.bar(player_counts, avg_win_amounts)
    plt.xlabel('Player Count')
    plt.ylabel('Average Win Amount')
    plt.title('Average Win Amount by Player Count (CassandraDB)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main3():
    session = connect_cassandra()
    rows_cassandra, execution_time = cassandra_select3(session)
    disconnect_cassandra(session)

    generate_avg_win_amount_plot(rows_cassandra)

    print("Execution Time:", execution_time, "seconds")

if __name__ == '__main__':
    main()
    main2()
    main3()

import psycopg2
import matplotlib.pyplot as plt
from baza_mongo import connect_mongo, disconnect_mongo, mongo_select

import time


def mongo_select(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie polecenia SELECT w bazie MongoDB
    cursor = db_mongo['games'].aggregate([
        {'$group': {'_id': '$date', 'count': {'$sum': 1}}}
    ])
    results = list(cursor)
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
    

    # Połączenie z bazą danych MongoDB
    db_mongo, client_mongo = connect_mongo()
    rows_mongo, execution_time_mongo = mongo_select(db_mongo)
    disconnect_mongo(client_mongo)


    generate_game_count_plot(rows_mongo)

    print("Execution Time (MongoDB):", execution_time_mongo, "seconds")

if __name__ == '__main__':
    main()
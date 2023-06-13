import matplotlib.pyplot as plt
from baza_mongo import connect_mongo, disconnect_mongo
import time 

def mongodb_select(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania w MongoDB
    pipeline = [
        {
            "$group": {
                "_id": "$date",
                "count": { "$sum": 1 }
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]
    result = db_mongo.games.aggregate(pipeline)
    rows_mongo = [(row["_id"], row["count"]) for row in result]
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_mongo, execution_time

def generate_game_count_plot(data):
    dates = [row[0] for row in data]
    game_counts = [row[1] for row in data]

    plt.bar(dates, game_counts)
    plt.xlabel('Date')
    plt.ylabel('Game Count')
    plt.title('Game Count by Date (Mongo)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main():
    db_mongo, client_mongo = connect_mongo()
    rows_mongo, execution_time = mongodb_select(db_mongo)
    disconnect_mongo(client_mongo)

    generate_game_count_plot(rows_mongo)

    print("Execution Time:", execution_time, "seconds")



############ WYKRES 2 ############

def mongodb_select2(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania w MongoDB
    pipeline = [
        {
            "$group": {
                "_id": "$date",
                "total_win_amount": { "$sum": "$total_win_amount" }
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]
    result = db_mongo.games.aggregate(pipeline)
    rows_mongo = [(row["_id"], row["total_win_amount"]) for row in result]
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_mongo, execution_time

def generate_total_win_amount_plot(data):
    dates = [row[0] for row in data]
    total_win_amounts = [row[1] for row in data]

    plt.plot(dates, total_win_amounts)
    plt.xlabel('Date')
    plt.ylabel('Total Win Amount')
    plt.title('Total Win Amount by Date')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main2():
    db_mongo, client_mongo = connect_mongo()
    rows_mongo, execution_time = mongodb_select2(db_mongo)
    disconnect_mongo(client_mongo)

    generate_total_win_amount_plot(rows_mongo)

    print("Execution Time:", execution_time, "seconds")



def mongodb_select3(db_mongo):
    start_time = time.time()  # Początkowy czas wykonania zapytania
    # Wykonanie zapytania w MongoDB
    pipeline = [
        {
            "$group": {
                "_id": "$player_count",
                "avg_win_amount": { "$avg": "$total_win_amount" }
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]
    result = db_mongo.games.aggregate(pipeline)
    rows_mongo = [(row["_id"], row["avg_win_amount"]) for row in result]
    end_time = time.time()  # Końcowy czas wykonania zapytania
    execution_time = end_time - start_time  # Czas wykonania zapytania
    return rows_mongo, execution_time

def generate_avg_win_amount_plot(data):
    player_counts = [row[0] for row in data]
    avg_win_amounts = [row[1] for row in data]

    plt.bar(player_counts, avg_win_amounts)
    plt.xlabel('Player Count')
    plt.ylabel('Average Win Amount')
    plt.title('Average Win Amount by Player Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main3():
    db_mongo, client_mongo = connect_mongo()
    rows_mongo, execution_time = mongodb_select3(db_mongo)
    disconnect_mongo(client_mongo)

    generate_avg_win_amount_plot(rows_mongo)

    print("Execution Time:", execution_time, "seconds")



if __name__ == '__main__':
    main()
    main2()    
    main3()

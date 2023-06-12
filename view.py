from flask import Flask, render_template
from baza_sql import connect_sql, sql_select, sql_insert, sql_delete, sql_update, disconnect_sql
from baza_mongo import connect_mongo, mongo_select, disconnect_mongo, mongo_insert, mongo_delete, mongo_update
from baza_cassandra import connect_cassandra, cassandra_select, cassandra_insert, cassandra_delete, cassandra_update, disconnect_cassandra
import os

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/test_select/')
def test_select():
    return render_template('select.html')

@app.route('/test_insert/')
def test_insert():
    return render_template('insert.html')

@app.route('/test_delete/')
def test_delete():
    return render_template('delete.html')

@app.route('/test_update/')
def test_update():
    return render_template('update.html')


#DLA SELECT

@app.route('/wykonaj_select_sql/')
def wykonaj_select():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_select(cur_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('select.html', rows_pg=rows_pg)

@app.route('/wykonaj_select_mongo/')
def wykonaj_select_mongo():
    db_mongo, client_mongo = connect_mongo()
    cursor, execution_time = mongo_select(db_mongo)
    results = list(cursor)
    disconnect_mongo(client_mongo)
    return render_template('select.html', rows_pg=(results, execution_time))

@app.route('/wykonaj_select_cassandra/')
def wykonaj_select_cassandra():
    session = connect_cassandra()
    rows_cassandra, execution_time = cassandra_select(session)
    disconnect_cassandra(session)
    return render_template('select.html', rows_pg=(rows_cassandra, execution_time))



#DLA INSERT

@app.route('/wykonaj_insert_sql/')
def wykonaj_insert():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_insert(cur_pg, conn_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('insert.html', rows_pg=rows_pg)

@app.route('/wykonaj_insert_mongo/')
def wykonaj_insert_mongo():
    db_mongo, client_mongo = connect_mongo()
    rows_pg = mongo_insert(db_mongo)
    disconnect_mongo(client_mongo)
    return render_template('insert.html', rows_pg=rows_pg)

@app.route('/wykonaj_insert_cassandra/')
def wykonaj_insert_cassandra():
    session = connect_cassandra()
    execution_time = cassandra_insert(session)
    disconnect_cassandra(session)
    return render_template('insert.html', rows_pg=execution_time)


#DLA DELETE

@app.route('/wykonaj_delete_sql/')
def wykonaj_delete():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_delete(cur_pg, conn_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('delete.html', rows_pg=rows_pg)


@app.route('/wykonaj_delete_mongo/')
def wykonaj_delete_mongo():
    db_mongo, client_mongo = connect_mongo()
    rows_pg = mongo_delete(db_mongo)
    disconnect_mongo(client_mongo)
    return render_template('delete.html', rows_pg=rows_pg)

@app.route('/wykonaj_delete_cassandra/')
def wykonaj_delete_cassandra():
    session = connect_cassandra()
    execution_time = cassandra_delete(session)
    disconnect_cassandra(session)
    return render_template('delete.html', rows_pg=execution_time)


#DLA UPDATE

@app.route('/wykonaj_update_sql/')
def wykonaj_update():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_update(cur_pg, conn_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('update.html', rows_pg=rows_pg)

@app.route('/wykonaj_update_mongo/')
def wykonaj_update_mongo():
    db_mongo, client_mongo = connect_mongo()
    rows_pg = mongo_update(db_mongo)
    disconnect_mongo(client_mongo)
    return render_template('update.html', rows_pg=rows_pg)

@app.route('/wykonaj_update_cassandra/')
def wykonaj_update_cassandra():
    session = connect_cassandra()
    execution_time = cassandra_update(session)
    disconnect_cassandra(session)
    return render_template('update.html', rows_pg=execution_time)



if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)


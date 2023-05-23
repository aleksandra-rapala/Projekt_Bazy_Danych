from flask import Flask, render_template
from baza_sql import connect_sql, sql_select, sql_insert, sql_delete, sql_update, disconnect_sql
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


#DLA SQL

@app.route('/wykonaj_select_sql/')
def wykonaj_select():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_select(cur_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('select.html', rows_pg=rows_pg)

@app.route('/wykonaj_insert_sql/')
def wykonaj_insert():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_insert(cur_pg, conn_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('insert.html', rows_pg=rows_pg)

@app.route('/wykonaj_delete_sql/')
def wykonaj_delete():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_delete(cur_pg, conn_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('delete.html', rows_pg=rows_pg)

@app.route('/wykonaj_update_sql/')
def wykonaj_update():
    cur_pg, conn_pg = connect_sql()
    rows_pg = sql_update(cur_pg, conn_pg)
    disconnect_sql(cur_pg, conn_pg)
    return render_template('update.html', rows_pg=rows_pg)



if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)


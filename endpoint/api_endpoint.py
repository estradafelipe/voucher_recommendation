from flask import Flask, request
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

POSTGRES_DB = 'postgresql://admin:admin@localhost:5432/voucher_api'
CURRENT_DATE = '2020-05-20 00:00:00'

print(POSTGRES_DB)
connection = psycopg2.connect(POSTGRES_DB)

GLOBAL_AVG = """SELECT * FROM model_prod.voucher_segmentation;"""

@app.route("/")
def hello():
    return "Hello, welcome to voucher api!"


@app.get("/all")
def get_global_avg():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(GLOBAL_AVG)
            data = cursor.fetchall()
    return {"data": data}

@app.get("/voucher")
def get_voucher():
    data_req = request.get_json()
    last_order = data_req.get('last_order_ts')
    segment_name = data_req.get('segment_name')
    total_orders = data_req.get('total_orders')
    datediff = abs((datetime.strptime(last_order, '%Y-%m-%d %H:%M:%S') -
                        datetime.strptime(CURRENT_DATE, '%Y-%m-%d %H:%M:%S')).days)
    query = """SELECT voucher_amount FROM model_prod.voucher_segmentation WHERE segment_type = %(segment_name)s AND %(dimension)s BETWEEN lower_floor AND upper_floor"""
    #query = query.format(segment_name=str(segment_name), dimension=datediff)
    if segment_name == 'recency_segment':
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(query , {'segment_name':segment_name,'dimension':datediff})
                data = cursor.fetchone()[0]
    elif segment_name == 'frequent_segment':
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(query , {'segment_name':segment_name,'dimension':total_orders})
                data = cursor.fetchone()[0]
    return {"voucher_amount": data}
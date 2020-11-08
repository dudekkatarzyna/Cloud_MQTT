# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import json
import pymysql as pymysql
import config

host = config.host
port = config.port
user = config.user
password = config.password
database = config.database


# ===============================================================
# Database Manager Class

class DatabaseManager():
    def __init__(self):
        self.conn = pymysql.connect(host, user, password, database)
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT VERSION()")
        version = self.cur.fetchone()

    def add_del_update_db_record(self, sql_query, args=()):
        self.cur.execute("SELECT VERSION()")
        version = self.cur.fetchone()
        self.cur.execute(sql_query)
        self.conn.commit()
        return

    def __del__(self):
        self.cur.close()
        self.conn.close()


# ===============================================================
# Functions to push Sensor Data into Database

buffer_values = []


def reset_counter():
    global buffer_values, buffer_index

    buffer_values = []


def DHT22_Data_Handler(jsonData):
    global buffer_values, buffer_index

    print("Received data...")
    json_Dict = json.loads(jsonData)
    for data in json_Dict:
        ShopID = int(data['ShopID'])
        Data_and_Time = data['Date_n_Time']
        CurrentCapacity = data['CurrentCapacity']

        buffer_values.append(
            (("NULL", ShopID, Data_and_Time, CurrentCapacity)))

    dbObj = DatabaseManager()
    values = ', '.join(map(str, buffer_values))
    dbObj.add_del_update_db_record("INSERT INTO SensorData VALUES  {}".format(values))
    print("Data inserted into database.")
    del dbObj

    reset_counter()


def sensor_Data_Handler(Topic, jsonData):
    if Topic == "cloud2020/kdudek/shopping_center":
        DHT22_Data_Handler(jsonData)

# ===============================================================

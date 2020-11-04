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

buffer_index = 0
buffer_values = []


def reset_counter():
    global buffer_values, buffer_index

    buffer_values = []
    buffer_index = 0


def check_storage():
    global buffer_values, buffer_index
    if buffer_index == 10:
        dbObj = DatabaseManager()
        values = ', '.join(map(str, buffer_values))
        dbObj.add_del_update_db_record("INSERT INTO SensorData VALUES  {}".format(values))
        del dbObj
        reset_counter()
        print("Inserted Data into Database.")


def DHT22_Data_Handler(jsonData):
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Temperature = json_Dict['Temperature']
    Humidity = json_Dict['Humidity']
    Pollution = json_Dict['Pollution']
    Location = json_Dict['Location']

    global buffer_values, buffer_index
    buffer_values.append(
        (("NULL", SensorID, Data_and_Time, str(Temperature), str(Humidity), str(Pollution), str(Location))))
    buffer_index += 1

    check_storage()

def sensor_Data_Handler(Topic, jsonData):
    if Topic == "cloud2020/kdudek/sensor_data":
        DHT22_Data_Handler(jsonData)

# ===============================================================

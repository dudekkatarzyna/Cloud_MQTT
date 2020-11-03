# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import json
import sqlite3

# SQLite DB Name
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
        print("Database version: {} ".format(version[0]))

    # self.conn.execute('pragma foreign_keys = on')
    # self.conn.commit()
    # self.cur = self.conn.cursor()

    def add_del_update_db_record(self, sql_query, args=()):
        print(args)
        self.cur.execute("SELECT VERSION()")
        version = self.cur.fetchone()
        print("Database version: {} ".format(version[0]))
        self.cur.execute(sql_query, args)
        self.conn.commit()
        return

    def __del__(self):
        self.cur.close()
        self.conn.close()


# ===============================================================
# Functions to push Sensor Data into Database

# Function to save Temperature to DB Table
def DHT22_Temp_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Temperature = json_Dict['Temperature']

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record(
        "insert into TemperatureData (SensorID, Date_n_Time, Temperature) values (%s,%s,%s)",
        [SensorID, Data_and_Time, str(Temperature)])
    del dbObj
    print("Inserted Temperature Data into Database.")
    print("")


# Function to save Humidity to DB Table
def DHT22_Humidity_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Humidity = json_Dict['Humidity']

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into HumidityData (SensorID, Date_n_Time, Humidity) values (%s,%s,%s)",
                                   [SensorID, Data_and_Time, str(Humidity)])
    del dbObj
    print("Inserted Humidity Data into Database.")
    print("")


def DHT22_Pollution_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Pollution = json_Dict['Pollution']

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into PollutionData (SensorID, Date_n_Time, Pollution) values (%s,%s,%s)",
                                   [SensorID, Data_and_Time, str(Pollution)])
    del dbObj
    print("Inserted Pollution Data into Database.")
    print("")


def DHT22_Location_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Location = json_Dict['Location']

    # Push into DB Table
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into LocationData (SensorID, Date_n_Time, Location) values (%s,%s,%s)",
                                   [SensorID, Data_and_Time, str(Location)])
    del dbObj
    print("Inserted Location Data into Database.")
    print("")


# ===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
    if Topic == "cloud2020/kdudek/sensor_data/temperature":
        DHT22_Temp_Data_Handler(jsonData)
    elif Topic == "cloud2020/kdudek/sensor_data/humidity":
        DHT22_Humidity_Data_Handler(jsonData)
    elif Topic == "cloud2020/kdudek/sensor_data/pollution":
        DHT22_Pollution_Data_Handler(jsonData)
    elif Topic == "cloud2020/kdudek/sensor_data/location":
        DHT22_Location_Data_Handler(jsonData)

# ===============================================================

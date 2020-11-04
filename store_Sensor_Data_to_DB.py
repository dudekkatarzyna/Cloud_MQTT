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
temperature_Values = []
humidity_Values = []
pollution_Values = []
location_Values = []


def reset_counter():
    global temperature_Values, humidity_Values, pollution_Values, location_Values, buffer_index

    temperature_Values = []
    humidity_Values = []
    pollution_Values = []
    location_Values = []
    buffer_index = 0


def check_storage():
    global temperature_Values, humidity_Values, pollution_Values, location_Values, buffer_index
    if buffer_index == 10:
        dbObj = DatabaseManager()

        if temperature_Values:
            values = ', '.join(map(str, temperature_Values))
            dbObj.add_del_update_db_record("INSERT INTO TemperatureData VALUES  {}".format(values))

        if humidity_Values:
            values = ', '.join(map(str, humidity_Values))
            dbObj.add_del_update_db_record("INSERT INTO HumidityData VALUES  {}".format(values))

        if pollution_Values:
            values = ', '.join(map(str, pollution_Values))
            dbObj.add_del_update_db_record("INSERT INTO PollutionData VALUES  {}".format(values))

        if location_Values:
            values = ', '.join(map(str, location_Values))
            dbObj.add_del_update_db_record("INSERT INTO LocationData VALUES  {}".format(values))
        del dbObj

        reset_counter()
        print("Inserted Data into Database.")


# Function to save Temperature to DB Table
def DHT22_Temp_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Temperature = json_Dict['Temperature']

    global temperature_Values, buffer_index
    temperature_Values.append((("NULL", SensorID, Data_and_Time, str(Temperature))))
    buffer_index += 1

    check_storage()


# Function to save Humidity to DB Table
def DHT22_Humidity_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Humidity = json_Dict['Humidity']

    global humidity_Values, buffer_index
    humidity_Values.append((("NULL", SensorID, Data_and_Time, str(Humidity))))
    buffer_index += 1

    check_storage()


def DHT22_Pollution_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Pollution = json_Dict['Pollution']

    global pollution_Values, buffer_index
    pollution_Values.append((("NULL", SensorID, Data_and_Time, str(Pollution))))
    buffer_index += 1

    check_storage()


def DHT22_Location_Data_Handler(jsonData):
    # Parse Data
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Location = json_Dict['Location']

    global location_Values, buffer_index
    location_Values.append((("NULL", SensorID, Data_and_Time, str(Location))))
    buffer_index += 1

    check_storage()


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

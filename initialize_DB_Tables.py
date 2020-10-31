# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------
import pymysql
import config

host = config.host
port = config.port
user = config.user
password = config.password
database = config.database

TemperatureTableSchema = """
CREATE TABLE TemperatureData (
    DataID int,
    SensorID varchar(255),
     Date_n_Time date,
    Temperature varchar(255)
);
"""
HumidityTableSchema = """
CREATE TABLE HumidityData (
    DataID int,
    SensorID varchar(255),
    Date_n_Time date,
    Humidity varchar(255)
);
"""
PollutionTableSchema = """
CREATE TABLE PollutionData (
    DataID int,
    SensorID varchar(255),
     Date_n_Time date,
    Pollution varchar(255)
);
"""
LocationTableSchema = """

CREATE TABLE LocationData (
    DataID int,
    SensorID varchar(255),
    Date_n_Time date,
    Location varchar(255)
);

"""
# Connect or Create DB File
conn = pymysql.connect(host, user, password, database, port)
cur = conn.cursor()
cur.execute("SELECT VERSION()")
version = cur.fetchone()
print("Database version: {} ".format(version[0]))
curs = conn.cursor()

# Create Tables
curs.execute(TemperatureTableSchema)
curs.execute(HumidityTableSchema)
curs.execute(PollutionTableSchema)
curs.execute(LocationTableSchema)

# Close DB
curs.close()
conn.close()

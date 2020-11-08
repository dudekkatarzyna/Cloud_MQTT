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

TableSchema = """
CREATE TABLE Shop (
    id INT NOT NULL AUTO_INCREMENT KEY,
    Name varchar(255),
    MaxCapacity INT 
);
CREATE TABLE SensorData (
    id INT NOT NULL AUTO_INCREMENT KEY,
    ShopID INT,
    Date_n_Time datetime,
    CurrentCapacity INT,
    FOREIGN KEY (ShopID) references  Shop(id)
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
curs.execute(TableSchema)


# Close DB
curs.close()
conn.close()

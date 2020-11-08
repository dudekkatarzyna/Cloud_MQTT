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
INSERT INTO Shop VALUES 
(NULL, 'The Torrance Council', 400),
(NULL, 'Vader Corp', 260),
(NULL, 'Vader and Co', 130),
(NULL, 'McMark Superhalks', 260),
(NULL, 'Blunder Industries', 60),
(NULL, 'Parker and Co', 60),
(NULL, 'Parker Corp', 400),
(NULL, 'Parker of Devon', 60),
(NULL, 'Piping Taught', 60),
(NULL, 'Parting Taught', 130),
(NULL, 'Taught on the Heels', 260),
(NULL, 'Lifes Too Support', 260),
(NULL, 'Taught Rocket Science', 130),
(NULL, 'A Taught Potato', 260),
(NULL, 'Barking Ad', 130),
(NULL, 'Tissue of Advertise', 260),
(NULL, 'Rabbit of Africa', 130),
(NULL, 'Bogtrotter Industries', 60),
(NULL, 'Olsson Unlimited', 60),
(NULL, 'The Fifth', 400),
(NULL, 'The Decorate Gatsby', 260),
(NULL, 'The Fifth Decorate', 400),
(NULL, 'Off the Award', 130),
(NULL, 'Clean Decorate', 260),
(NULL, 'Ad as a Hatter', 60),
(NULL, 'Advertise and Shine', 60),
(NULL, 'Twist of Decorate', 260),
(NULL, 'Ad Egg', 260),
(NULL, 'Blind Decorate', 130),
(NULL, 'If I Ad My Druthers', 400);

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

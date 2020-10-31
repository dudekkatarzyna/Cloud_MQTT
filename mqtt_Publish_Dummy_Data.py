# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime
from near_location import near_location
import sys

# ====================================================
# MQTT Settings 
MQTT_Broker = "test.mosquitto.org"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic_Humidity = "cloud2020/kdudek/sensor_data/humidity"
MQTT_Topic_Temperature = "cloud2020/kdudek/sensor_data/temperature"
MQTT_Topic_Pollution = "cloud2020/kdudek/sensor_data/pollution"
MQTT_Topic_Location = "cloud2020/kdudek/sensor_data/location"


# ====================================================

def on_connect(client, userdata, rc):
    if rc != 0:
        pass
        print("Unable to connect to MQTT Broker...")
    else:
        print("Connected with MQTT Broker: " + str(MQTT_Broker))


def on_publish(client, userdata, mid):
    print("on_publish")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        pass


mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))


def publish_To_Topic(topic, message):
    mqttc.publish(topic, message)
    print("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
    print("")


# ====================================================
# FAKE SENSOR 
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker


toggle = int(sys.argv[1])


def publish_Fake_Sensor_Values_to_MQTT():
    threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()
    global toggle
    if toggle == 0:
        Humidity_Fake_Value = float("{0:.2f}".format(random.uniform(10, 90)))

        Humidity_Data = {}
        Humidity_Data['Sensor_ID'] = "s1"
        Humidity_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        Humidity_Data['Humidity'] = Humidity_Fake_Value
        humidity_json_data = json.dumps(Humidity_Data)

        print("Publishing fake Humidity Value: " + str(Humidity_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Humidity, humidity_json_data)

    elif toggle == 1:
        Temperature_Fake_Value = float("{0:.2f}".format(random.uniform(-20, 60)))

        Temperature_Data = {}
        Temperature_Data['Sensor_ID'] = "s2"
        Temperature_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        Temperature_Data['Temperature'] = Temperature_Fake_Value
        temperature_json_data = json.dumps(Temperature_Data)

        print("Publishing fake Temperature Value: " + str(Temperature_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Temperature, temperature_json_data)

    elif toggle == 2:
        Pollution_Fake_Value = float("{0:.2f}".format(random.uniform(0, 65)))

        Pollution_Data = {}
        Pollution_Data['Sensor_ID'] = "s3"
        Pollution_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        Pollution_Data['Pollution'] = Pollution_Fake_Value
        pollution_json_data = json.dumps(Pollution_Data)

        print("Publishing fake Pollution Value: " + str(Pollution_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Pollution, pollution_json_data)

    elif toggle == 3:
        Location_Fake_Value = near_location(float("{0:.2f}".format(random.uniform(-180, 180))),
                                            float("{0:.2f}".format(random.uniform(-180, 180))), 50)

        Location_Data = {}
        Location_Data['Sensor_ID'] = "s4"
        Location_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
        Location_Data['Location'] = Location_Fake_Value
        location_json_data = json.dumps(Location_Data)

        print("Publishing fake Location Value: " + str(Location_Fake_Value) + "...")
        publish_To_Topic(MQTT_Topic_Location, location_json_data)


publish_Fake_Sensor_Values_to_MQTT()

# ====================================================

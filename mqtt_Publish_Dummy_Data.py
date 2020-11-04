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
MQTT_Topic = "cloud2020/kdudek/sensor_data"


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


sensor_id = sys.argv[1]
Location_Fake_Value = near_location(float("{0:.2f}".format(random.uniform(-180, 180))),
                                    float("{0:.2f}".format(random.uniform(-180, 180))), 50)
def publish_Fake_Sensor_Values_to_MQTT():
    global Location_Fake_Value, sensor_id
    threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()

    Temperature_Fake_Value = float("{0:.2f}".format(random.uniform(-20, 60)))
    Humidity_Fake_Value = float("{0:.2f}".format(random.uniform(10, 90)))
    Pollution_Fake_Value = float("{0:.2f}".format(random.uniform(0, 65)))

    Data = {}
    Data['Sensor_ID'] = sensor_id
    Data['Date'] = (datetime.today()).strftime("%Y-%m-%d %H:%M:%S")
    Data['Temperature'] = Temperature_Fake_Value
    Data['Humidity'] = Humidity_Fake_Value
    Data['Pollution'] = Pollution_Fake_Value
    Data['Location'] = Location_Fake_Value

    json_data = json.dumps(Data)

    print("Publishing fake Values: " + str(json_data) + "...")
    publish_To_Topic(MQTT_Topic, json_data)


publish_Fake_Sensor_Values_to_MQTT()

# ====================================================

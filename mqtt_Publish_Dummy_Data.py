# ------------------------------------------
# --- Author: Pradeep Singh
# --- Date: 20th January 2017
# --- Version: 1.0
# --- Python Ver: 2.7
# --- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
# ------------------------------------------


import paho.mqtt.client as mqtt
import random, threading, json
import sys

# ====================================================
# MQTT Settings


from fake_Publish_Data import fake_Publish_Data

MQTT_Broker = "test.mosquitto.org"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic = "cloud2020/kdudek/shopping_center"


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


def publish_Fake_Sensor_Values_to_MQTT():
    threading.Timer(30.0, publish_Fake_Sensor_Values_to_MQTT).start()

    Data = fake_Publish_Data()
    json_data = json.dumps(Data)

    print("Publishing fake Values: " + str(json_data) + "...")
    publish_To_Topic(MQTT_Topic, json_data)


publish_Fake_Sensor_Values_to_MQTT()

# ====================================================

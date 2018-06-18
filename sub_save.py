#-----------------------------
#<-------- Libraries -------->
#-----------------------------
import paho.mqtt.client as mqtt
import json
import MySQLdb
import time
import datetime
import dateutil.parser
import base64


#-----------------------------
#<------ Configuration ------>
#-----------------------------
#TTN Configuration
TTN_appeui = "app_EUI"
TTN_appid  = "app_ID"
TTN_password = 'ttn-account-v2.<lots_of_chars>'
TTN_tls_path = 'mqtt-ca.pem'

#MySQL Configuration
MySQL_host = "localhost"
MySQL_user = "user"
MySQL_password = "password"
MySQL_db = "db"


#-----------------------------
#<-------- Functions -------->
#-----------------------------

def on_connect(mqttc, mosq, obj, rc):
    print("Connected with result code:"+str(rc))
    # subscribe to specific device in a specific app
    mqttc.subscribe('app_ID/devices/my_device/up')
    # subscribe to all devices in a specific app
    #mqttc.subscribe('app_ID/devices/+/up')


def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed")


def on_message(mqttc, obj, msg):
    try:
        x = json.loads(msg.payload.decode('utf-8'))
        device = str(x["dev_id"])
        payload = int(base64.b64decode(x["payload_raw"])) #assume payload is the value of a sensor
        datetime = int(time.mktime(dateutil.parser.parse(x["metadata"]["time"]).timetuple())) #transform to Unix epoch format

        cursor.execute("""INSERT INTO test VALUES (%s, %s, %s)""", (device, datetime, payload))
        db.commit()
    except Exception as e:
        print(e)
        db.rollback()
        pass


#-----------------------------
#<---------- Main ----------->
#-----------------------------
mqttc= mqtt.Client()
db = MySQLdb.connect(host=MySQL_host, user=MySQL_user, passwd=MySQL_password, db=MySQL_db)
cursor = db.cursor()

# Assign event callbacks
mqttc.on_connect=on_connect
mqttc.on_message=on_message

mqttc.username_pw_set(APPID, PSW)
mqttc.tls_set(TTN_tls_path)
mqttc.connect("eu.thethings.network", 8883, 10)

mqttc.loop_forever()
db.close()

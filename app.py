from glob import glob
import threading
import paho.mqtt.client as mqtt
import MySQLdb
import json
from os import path
from csv import writer
import time
from datetime import datetime
from datetime import timedelta
import time
import random
import math


counter = 0
DataUpdatePeriod = 3
SQLUpdatePeriod = 300

DeviceValidityCheck = False

thread = None
BackgroundThread = None
MonitoringThread = None
WebsiteUpdateThread = None
DataStreamThread = None
SQLWriterThread = None

client = mqtt.Client("Python")

SQLHost = "192.168.1.10"
SQLUser = "root"
SQLPasswd = "123456"
SQLDB = "SmartRoomData"

today = datetime.now()

deviceTemplates = {}
deviceStatus = {}
Status = {}
StatusNET = {}
StatusFWR = {}
StatusPRM = {}
onlineDevices = {}

DataTableNames = {
  "DHT": "ESP_DHT_STATUS",
  "MINIR2": "SONOFF_MINIR2_STATUS",
  "DUALR3": "SONOFF_DUALR3_STATUS",
  "POW316": "SONOFF_POW316_STATUS"
}

# MQTT emit formatted data


def retransmitFormattedData(message):
    global deviceTemplates, deviceStatus, onlineDevices
    global DeviceValidityCheck

    try:
        message["Template"] = json.loads(
            deviceTemplates[message["DeviceType"]])
    except:
        DeviceValidityCheck = False
        return

    try:
        message["DeviceInfo"] = deviceStatus[message["DeviceID"]]
    except:
        DeviceValidityCheck = False
        return

    DeviceValidityCheck = True
    onlineDevices[message["DeviceID"]] = message



# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    client.subscribe("tele/+/+")
    client.subscribe("stat/+/+")

# The callback for when a PUBLISH message is received from the server.

def evalStatusFeedbackMessages(Topics, Message):
    Value = Topics[2].replace("POWER", "Relay1")
    try:
        if (Message == "ON"):
            print("Setting " + Topics[2] + "to 1")
            onlineDevices[Topics[1][-6:]]["Data"][Value] = 1
            
        if (Message == "OFF"):
            print("Setting " + Topics[2] + "to 0")
            onlineDevices[Topics[1][-6:]]["Data"][Value] = 0
    except:
        print("Zariadenie neexistuje")        
    print(Message)

def on_message(client, userdata, msg):
    global SonoffMINISTATETemp, SonoffDualSTATETemp, SonoffPOW316STATETemp, counter
    global Status1Messages, Status2Messages, Status5Messages

    Topics = msg.topic.split('/')

    try:
        DataJSON = json.loads(msg.payload.decode("utf-8"))
    except:
        if ("POWER" in Topics[2]):
            evalStatusFeedbackMessages(Topics, msg.payload.decode("utf-8"))
            return
        print("JSON could not be decoded")
        print(Topics)
        print(msg.payload.decode("utf-8"))
        return

    # save to MySQL
    # telemetry messages
    if (Topics[0] == "tele"):
        # DHT Temperature, Humidity messages
        if "DHT" in Topics[1]:
            # retransmitting formatted data
            retransmitJSONData = {
                "Humidity": DataJSON["DHT11"]["Humidity"],
                "Temperature": DataJSON["DHT11"]["Temperature"]
            }

            retransmitJSON = {

                "Time": DataJSON["Time"],
                "DeviceID": DataJSON["DeviceID"][-6:],
                "DeviceName": DataJSON["DeviceID"],
                "DeviceType": "DHT",
                "DataTableName": "ESP_DHT_STATUS",
                "Data": retransmitJSONData
            }

            retransmitFormattedData(retransmitJSON)
            return

        # Sonoff Mini R2
        if "MINIR2" in Topics[1]:
            if "STATE" in Topics[2]:
                SonoffMINISTATETemp = {
                    "MsgTopic": msg.topic[:-6],
                    "ArrivalTime": DataJSON["Time"],
                    "Message": DataJSON
                }
                return

            if "SENSOR" in Topics[2]:
                if (SonoffMINISTATETemp["MsgTopic"] == msg.topic[:-7]) and (SonoffMINISTATETemp["ArrivalTime"] == DataJSON["Time"]):
                    # retransmitting formatted data
                    if SonoffMINISTATETemp["Message"]["POWER"] == "ON":
                        POW = 1
                    else:
                        POW = 0

                    if DataJSON["Switch1"] == "ON":
                        SW = 1
                    else:
                        SW = 0

                    retransmitJSONData = {
                        "Relay1": POW,
                        "Switch": SW
                    }

                    retransmitJSON = {

                        "Time": DataJSON["Time"],
                        "DeviceID": msg.topic[5:-7][-6:],
                        "DeviceName": msg.topic[5:-7],
                        "DeviceType": "MINIR2",
                        "DataTableName": "SONOFF_MINIR2_STATUS",
                        "Data": retransmitJSONData
                    }

                    retransmitFormattedData(retransmitJSON)
                    return

        # Sonoff DualR3
        if "DUALR3" in Topics[1]:
            if "STATE" in Topics[2]:
                SonoffDualSTATETemp = {
                    "MsgTopic": msg.topic[:-6],
                    "ArrivalTime": DataJSON["Time"],
                    "Message": DataJSON
                }
                return

            if "SENSOR" in Topics[2]:
                if (SonoffDualSTATETemp["MsgTopic"] == msg.topic[:-7]) and (SonoffDualSTATETemp["ArrivalTime"] == DataJSON["Time"]):
                    # retransmitting formatted data
                    if SonoffDualSTATETemp["Message"]["POWER1"] == "ON":
                        POW1 = 1
                    else:
                        POW1 = 0
                    if SonoffDualSTATETemp["Message"]["POWER2"] == "ON":
                        POW2 = 1
                    else:
                        POW2 = 0
                    if DataJSON["Switch1"] == "ON":
                        SW1 = 1
                    else:
                        SW1 = 0
                    if DataJSON["Switch2"] == "ON":
                        SW2 = 1
                    else:
                        SW2 = 0

                    retransmitJSONData = {
                        "Switch1": SW1,
                        "Switch2": SW2,
                        "Relay1": POW1,
                        "Relay2": POW2,
                        "Power1": DataJSON["ENERGY"]["Power"][0],
                        "Power2": DataJSON["ENERGY"]["Power"][1],
                        "Factor1": DataJSON["ENERGY"]["Factor"][0],
                        "Factor2": DataJSON["ENERGY"]["Factor"][1],
                        "Period1": DataJSON["ENERGY"]["Period"][0],
                        "Period2": DataJSON["ENERGY"]["Period"][1],
                        "Voltage": DataJSON["ENERGY"]["Voltage"],
                        "Current1": DataJSON["ENERGY"]["Current"][0],
                        "Current2": DataJSON["ENERGY"]["Current"][1],
                        "ApparentPower1": DataJSON["ENERGY"]["ApparentPower"][0],
                        "ApparentPower2": DataJSON["ENERGY"]["ApparentPower"][1],
                        "PowerUsedToday": DataJSON["ENERGY"]["Today"],
                        "PowerUsedTotal": DataJSON["ENERGY"]["Total"],
                        "ReactivePower1": DataJSON["ENERGY"]["ReactivePower"][0],
                        "ReactivePower2": DataJSON["ENERGY"]["ReactivePower"][1]
                    }

                    retransmitJSON = {

                        "Time": DataJSON["Time"],
                        "DeviceID": msg.topic[5:-7][-6:],
                        "DeviceName": msg.topic[5:-7],
                        "DeviceType": "DUALR3",
                        "DataTableName": "SONOFF_DUALR3_STATUS",
                        "Data": retransmitJSONData
                    }
                    retransmitFormattedData(retransmitJSON)
                    return

        # Sonoff POW316
        if "POW316" in Topics[1]:
            if "STATE" in Topics[2]:
                SonoffPOW316STATETemp = {
                    "MsgTopic": msg.topic[:-6],
                    "ArrivalTime": DataJSON["Time"],
                    "Message": DataJSON
                }
                return

            if "SENSOR" in Topics[2]:
                if (SonoffPOW316STATETemp["MsgTopic"] == msg.topic[:-7]) and (SonoffPOW316STATETemp["ArrivalTime"] == DataJSON["Time"]):

                    if SonoffPOW316STATETemp["Message"]["POWER"] == "ON":
                        POW = 1
                    else:
                        POW = 0

                    retransmitJSONData = {
                        "Relay1": POW,
                        "Factor": DataJSON["ENERGY"]["Factor"],
                        "Period": DataJSON["ENERGY"]["Period"],
                        "Voltage": DataJSON["ENERGY"]["Voltage"],
                        "Current": DataJSON["ENERGY"]["Current"],
                        "Power": DataJSON["ENERGY"]["Power"],
                        "ApparentPower": DataJSON["ENERGY"]["ApparentPower"],
                        "PowerUsedToday": DataJSON["ENERGY"]["Today"],
                        "PowerUsedTotal": DataJSON["ENERGY"]["Total"],
                        "ReactivePower": DataJSON["ENERGY"]["ApparentPower"]
                    }

                    retransmitJSON = {

                        "Time": DataJSON["Time"],
                        "DeviceID": msg.topic[5:-7][-6:],
                        "DeviceName": msg.topic[5:-7],
                        "DeviceType": "POW316",
                        "DataTableName": "SONOFF_POW316_STATUS",
                        "Data": retransmitJSONData
                    }

                    retransmitFormattedData(retransmitJSON)
                    return

    elif (Topics[0] == "stat"):
        evalStatusMessages(Topics, DataJSON, msg.topic)


def evalStatusMessages(Topics, DataJSON, topic):
    global Status, StatusNET, StatusPRM, StatusFWR
    today = datetime.now()

    DID = Topics[1][-6:]

    Status[DID]["DeviceID"] = DID
    Status[DID]["Time"] = today.isoformat()
    Status[DID]["ControlTopic"] = "cmnd/" + Topics[1]

    if (Topics[2] == "STATUS5"):
        StatusNET[DID]["Hostname"] = DataJSON["StatusNET"]["Hostname"]
        StatusNET[DID]["IPAddress"] = DataJSON["StatusNET"]["IPAddress"]
        StatusNET[DID]["Gateway"] = DataJSON["StatusNET"]["Gateway"]
        StatusNET[DID]["Subnetmask"] = DataJSON["StatusNET"]["Subnetmask"]
        StatusNET[DID]["Mac"] = DataJSON["StatusNET"]["Mac"]

    elif (Topics[2] == "STATUS2"):
        StatusFWR[DID]["Version"] = DataJSON["StatusFWR"]["Version"]
        StatusFWR[DID]["Hardware"] = DataJSON["StatusFWR"]["Hardware"]

    elif (Topics[2] == "STATUS1"):
        StatusPRM[DID]["GroupTopic"] = DataJSON["StatusPRM"]["GroupTopic"]
        StatusPRM[DID]["StartupUTC"] = DataJSON["StatusPRM"]["StartupUTC"]
        StatusPRM[DID]["RestartReason"] = DataJSON["StatusPRM"]["RestartReason"]

    try:
        deviceStatus[DID] = Status
        deviceStatus[DID]["StatusNET"] = StatusNET[DID]
        deviceStatus[DID]["StatusFWR"] = StatusFWR[DID]
        deviceStatus[DID]["StatusPRM"] = StatusPRM[DID]
    except:
        print("Insufficient data")


def DeviceTimeoutCheck():
    time.sleep(5)

    while True:
        offlineDevices = []
        today = datetime.now()
        timeouLimit = timedelta(minutes=1, seconds=30)
        for device in onlineDevices:
            # print(today - datetime.fromisoformat(onlineDevices[device]["Time"]))
            if ((today - datetime.fromisoformat(onlineDevices[device]["Time"])) > timeouLimit):
                offlineDevices.append(onlineDevices[device])

        for device in offlineDevices:
            print("Device " + device["DeviceName"] + " timed out")
            onlineDevices.pop(device["DeviceID"])
        time.sleep(10)


def background_device_monitor():
    time.sleep(2)
    while True:

        client.publish("cmnd/tasmotas/STATUS", 1)
        client.publish("cmnd/DHTs/STATUS", 1)
        time.sleep(2)

        client.publish("cmnd/tasmotas/STATUS", 2)
        client.publish("cmnd/DHTs/STATUS", 2)
        time.sleep(2)

        client.publish("cmnd/tasmotas/STATUS", 5)
        client.publish("cmnd/DHTs/STATUS", 5)

        time.sleep(54)


def on_disconnect():
    print("odpojene")


def background_thread():

    count = 0

    client.connect("192.168.1.63", 188)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.loop_forever()

    time.sleep(1)


def DataStreamThread():
    global client, onlineDevices, DeviceValidityCheck
 
    while True:
        if(DeviceValidityCheck == True):
            #Send Data for Device Listing
            deviceList = []
            for device in onlineDevices:
                deviceList.append(onlineDevices[device])
            if (len(deviceList) != 0):
                client.publish("data/stream", json.dumps(deviceList))
        time.sleep(DataUpdatePeriod)


def SQLWriterThread():
    global SQLHost, SQLUser, SQLPasswd, SQLDB
    global onlineDevices, DeviceValidityCheck

    while True:
        db = MySQLdb.connect(host=SQLHost, user=SQLUser,
                            passwd=SQLPasswd, db=SQLDB)
        cursor = db.cursor()

        getSQLDeviceTemplates(cursor)
        

        if(DeviceValidityCheck == True):
            SQLUpdateOnlineDevices(cursor, onlineDevices)

            for device in onlineDevices:
                
                if (onlineDevices[device]["DeviceType"] == 'DUALR3'):
                    [sql, val] = generate_DUALR3_SQL_Query(onlineDevices[device])
                elif (onlineDevices[device]["DeviceType"] == 'MINIR2'):
                    [sql, val] = generate_MINIR2_SQL_Query(onlineDevices[device])
                elif (onlineDevices[device]["DeviceType"] == 'POW316'):
                    [sql, val] = generate_POW316_SQL_Query(onlineDevices[device])
                elif (onlineDevices[device]["DeviceType"] == 'DHT'):
                    [sql, val] = generate_DHT_SQL_Query(onlineDevices[device])
                cursor.execute(sql, val)
                db.commit()
            
        db.close()
        time.sleep(SQLUpdatePeriod)

def SQLUpdateOnlineDevices(cursor, onlineDevices):
    cursor.execute("TRUNCATE TABLE `ONLINE_DEVICE_LIST`")
    for device in onlineDevices:
        sql = "INSERT INTO `ONLINE_DEVICE_LIST` (`DeviceID`, `Time`, `DeviceName`, `DataTableName`) VALUES (%s, %s, %s, %s);"
        data = (onlineDevices[device]["DeviceID"], onlineDevices[device]["Time"], onlineDevices[device]["DeviceName"], onlineDevices[device]["DataTableName"])
        cursor.execute(sql, data)
        print("Updated")
        


def getSQLDeviceTemplates(cursor):
    global deviceTemplates

    cursor.execute("SELECT DISTINCT DeviceType FROM `DEVICE_TEMPLATES`;")
    deviceTypes = cursor.fetchall()
    for Type in deviceTypes:
        cursor.execute(
            "SELECT Template FROM `DEVICE_TEMPLATES` WHERE DeviceType=%s;", [Type])
        deviceTemplate = cursor.fetchone()[0]
        deviceTemplates[Type[0]] = deviceTemplate


def generate_DUALR3_SQL_Query(device):
    TIME = device["Time"]
    DN = device["DeviceName"]
    DID = device["DeviceID"]
    Relay1 = device["Data"]["Power1"]
    Relay2 = device["Data"]["Power2"]
    SW1 = device["Data"]["Switch1"]
    SW2 = device["Data"]["Switch2"]
    PowerUsedTotal = device["Data"]["PowerUsedTotal"]	
    PowerUsedToday = device["Data"]["PowerUsedToday"]
    Period1 = device["Data"]["Period1"]
    Period2 = device["Data"]["Period2"]
    Power1 = device["Data"]["Power1"]
    Power2 = device["Data"]["Power2"]
    ApparentPower1 = device["Data"]["ApparentPower1"]
    ApparentPower2 = device["Data"]["ApparentPower2"]
    ReactivePower1 = device["Data"]["ReactivePower1"]
    ReactivePower2 = device["Data"]["ReactivePower2"]
    Factor1 = device["Data"]["Factor1"]
    Factor2 = device["Data"]["Factor2"]
    Voltage = device["Data"]["Voltage"]
    Current1 = device["Data"]["Current1"]
    Current2 = device["Data"]["Current2"]

    sql = "INSERT INTO `SONOFF_DUALR3_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `Relay1`, `Relay2`, `SW1`, `SW2`, `PowerUsedTotal`, `PowerUsedToday`, `Period1`, `Period2`, `Power1`, `Power2`, `ApparentPower1`, `ApparentPower2`, `ReactivePower1`, `ReactivePower2`, `Factor1`, `Factor2`, `Voltage`, `Current1`, `Current2`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    data = (TIME, DN, DID, Relay1, Relay2, SW1, SW2, PowerUsedTotal, PowerUsedToday, Period1, Period2, Power1, Power2, ApparentPower1, ApparentPower2, ReactivePower1, ReactivePower2, Factor1, Factor2, Voltage, Current1, Current2)
    
    return[sql, data]
    
def generate_POW316_SQL_Query(device):
    TIME = device["Time"]
    DN = device["DeviceName"]
    DID = device["DeviceID"]
    Relay1 = device["Data"]["Power"]
    PowerUsedTotal = device["Data"]["PowerUsedTotal"]	
    PowerUsedToday = device["Data"]["PowerUsedToday"]
    Period = device["Data"]["Period"]
    Power = device["Data"]["Power"]
    ApparentPower = device["Data"]["ApparentPower"]
    ReactivePower = device["Data"]["ReactivePower"]
    Factor = device["Data"]["Factor"]
    Voltage = device["Data"]["Voltage"]
    Current = device["Data"]["Current"]
  
    sql = "INSERT INTO `SONOFF_POW316_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `Relay1`, `PowerUsedTotal`, `PowerUsedToday`, `Period`, `Power`, `ApparentPower`, `ReactivePower`, `Factor`, `Voltage`, `Current`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    data = (TIME, DN, DID, Relay1, PowerUsedTotal, PowerUsedToday, Period, Power, ApparentPower, ReactivePower, Factor, Voltage, Current)
    return[sql, data]

def generate_MINIR2_SQL_Query(device):
    TIME = device["Time"]
    DN = device["DeviceName"]
    DID = device["DeviceID"]
    Relay1 = device["Data"]["Power"]
    SW = device["Data"]["Switch1"]

    sql = "INSERT INTO `SONOFF_MINIR2_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `Relay1`, `SWITCH` ) VALUES (NULL, %s, %s, %s, %s, %s);"
    data = (TIME, DN, DID, Relay1, SW)

    return[sql, data]


def generate_DHT_SQL_Query(device):
    TIME = device["Time"]
    DN = device["DeviceName"]
    DID = device["DeviceID"]
    TEMP = device["Data"]["Temperature"]
    HUM = device["Data"]["Humidity"]

    sql = "INSERT INTO `ESP_DHT_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `Temperature`, `Humidity`) VALUES (NULL, %s, %s, %s, %s, %s);"
    data = (TIME, DN, DID, TEMP, HUM)

    return[sql, data]

if __name__ == "__main__":
    MonitoringThread = threading.Thread(target=background_device_monitor, name='Monitor')
    MonitoringThread.start()
    DeviceUpdateThread = threading.Thread(target=DeviceTimeoutCheck, name='DeviceUpdate')
    DeviceUpdateThread.start()
    BackgroundThread = threading.Thread(target=background_thread, name='Background')
    BackgroundThread.start()
    DataStream = threading.Thread(target=DataStreamThread, name='DataStream')
    DataStream.start()
    SQLWriter = threading.Thread(target=SQLWriterThread, name='SQLWriter')
    SQLWriter.start()
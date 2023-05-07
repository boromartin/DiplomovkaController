import threading 
import paho.mqtt.client as mqtt
import MySQLdb    
import json
from os import path
from csv import writer
import time
from datetime import datetime



import time
import random
import math

counter = 0


myhost = "192.168.1.10"
myuser = "root"
mypasswd = "123456"
mydb = "SmartRoomData"


thread = None
BackgroundThread = None
MonitoringThread = None
WebsiteUpdateThread = None


client = mqtt.Client("Python")
db = MySQLdb.connect(host=myhost,user=myuser,passwd=mypasswd,db=mydb) 
cursor = db.cursor()
listObj = []
today = datetime.now()

Status1Messages = dict()
Status2Messages = dict()
Status5Messages = dict()

DataTableNames = {
  "DHT": "ESP_DHT_STATUS",
  "MINIR2": "SONOFF_MINIR2_STATUS",
  "DUALR3": "SONOFF_DUALR3_STATUS", 
  "POW316": "SONOFF_POW316_STATUS" 
}

OnlineDeviceTemplate = "('bad', datetime.datetime(2023, 4, 6, 15, 31, 2, 143146), 'bad', 'bad', 'bad', 'bad', None, None, None, None, None, None, None, None, None, None, None"


#   print(myhost)

#MQTT emit formatted data
def sendFormattedData (topic, message):
    client.publish(topic, json.dumps(message)) 
    #print()


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("tele/+/+")
    client.subscribe("stat/+/+")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    Topics = msg.topic.split('/')
    global SonoffMINISTATETemp, SonoffDualSTATETemp, SonoffPOW316STATETemp, counter
    global Status1Messages, Status2Messages, Status5Messages
    #print(SonoffSTATETemp)
    #socketio.emit('my_response',str(msg.payload)[2:-1],namespace='/test')
    global cursor
    try:
        DataJSON = json.loads(msg.payload.decode("utf-8"))
    except:
        print("JSON could not be decoded")
        print(Topics)
        print(msg.payload.decode("utf-8"))
        return
        
    #save to MySQL
    #telemetry messages
    if (Topics[0] == "tele"):
        #DHT Temperature, Humidity messages
        if "DHT" in Topics[1]:
            TIME = DataJSON["Time"]
            DN = DataJSON["DeviceID"]
            DID = DN[-6:]
            TEMP = DataJSON["DHT11"]["Temperature"]
            HUM = DataJSON["DHT11"]["Humidity"]
            
            sql = "INSERT INTO `ESP_DHT_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `Temperature`, `Humidity`) VALUES (NULL, %s, %s, %s, %s, %s);"
            data = (TIME, DN, DID, TEMP, HUM)
            
            cursor.execute(sql, data)
            db.commit()
            #print("DHT")
            
            #retransmitting formatted data
            retransmitJSONData = {
                "Humidity": HUM, 
                "Temperature": TEMP
            }
            
            retransmitJSON = {
            
                "Time": TIME, 
                "DeviceID": DID, 
                "DeviceName": DN, 
                "DeviceType": "DHT", 
                "Data": retransmitJSONData
            }
            
            sendFormattedData ("data/"+Topics[1], retransmitJSON)
            return
        
        #Sonoff Mini R2
        if "MINIR2" in Topics[1]:
            if "STATE" in Topics[2]:
                SonoffMINISTATETemp = {
                    "MsgTopic":msg.topic[:-6], 
                    "ArrivalTime": DataJSON["Time"], 
                    "Message": DataJSON
                }
                return
            
            if "SENSOR" in Topics[2]:
                if (SonoffMINISTATETemp["MsgTopic"] == msg.topic[:-7]) and (SonoffMINISTATETemp["ArrivalTime"] == DataJSON["Time"]):
                    TIME = DataJSON["Time"]
                    DN = msg.topic[5:-7]
                    DID = DN[-6:]
                    if SonoffMINISTATETemp["Message"]["POWER"] == "ON":
                        POW = 1
                    else:
                        POW = 0
                        
                    if DataJSON["Switch1"] == "ON":
                        SW = 1
                    else:
                        SW = 0                
                    sql = "INSERT INTO `SONOFF_MINIR2_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `POWER`, `SWITCH` ) VALUES (NULL, %s, %s, %s, %s, %s);"
                    data = (TIME, DN, DID, POW, SW)
                    
                    cursor.execute(sql, data)
                    db.commit()
                    #print("MINIR2")
                    
                    #retransmitting formatted data
                    retransmitJSONData = {
                        "Power": POW,
                        "Switch": SW
                    }
                    
                    retransmitJSON = {
                    
                        "Time": TIME, 
                        "DeviceID": DID, 
                        "DeviceName": DN, 
                        "DeviceType": "MINIR2", 
                        "Data": retransmitJSONData
                    }
                    
                    sendFormattedData ("data/"+Topics[1], retransmitJSON)
                    
                    return
                    
        #Sonoff DualR3
        if "DUALR3" in Topics[1]:
            if "STATE" in Topics[2]:
                SonoffDualSTATETemp = {
                    "MsgTopic":msg.topic[:-6], 
                    "ArrivalTime": DataJSON["Time"], 
                    "Message": DataJSON
                }
                return
            
            if "SENSOR" in Topics[2]:
                if (SonoffDualSTATETemp["MsgTopic"] == msg.topic[:-7]) and (SonoffDualSTATETemp["ArrivalTime"] == DataJSON["Time"]):
                    TIME = DataJSON["Time"]
                    DN = msg.topic[5:-7]
                    DID = DN[-6:]
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
                    
                    PowerUsedTotal = DataJSON["ENERGY"]["Total"]	
                    PowerUsedToday = DataJSON["ENERGY"]["Today"]
                    Period1 = DataJSON["ENERGY"]["Period"][0]
                    Period2 = DataJSON["ENERGY"]["Period"][1]
                    Power1 = DataJSON["ENERGY"]["Power"][0]
                    Power2 = DataJSON["ENERGY"]["Power"][1]
                    ApparentPower1 = DataJSON["ENERGY"]["ApparentPower"][0]
                    ApparentPower2 = DataJSON["ENERGY"]["ApparentPower"][1]
                    ReactivePower1 = DataJSON["ENERGY"]["ApparentPower"][0]
                    ReactivePower2 = DataJSON["ENERGY"]["ApparentPower"][1]
                    Factor1 = DataJSON["ENERGY"]["Factor"][0]
                    Factor2 = DataJSON["ENERGY"]["Factor"][1]
                    Voltage = DataJSON["ENERGY"]["Voltage"]
                    Current1 = DataJSON["ENERGY"]["Current"][0]
                    Current2 = DataJSON["ENERGY"]["Current"][1]

                    sql = "INSERT INTO `SONOFF_DUALR3_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `POW1`, `POW2`, `SW1`, `SW2`, `PowerUsedTotal`, `PowerUsedToday`, `Period1`, `Period2`, `Power1`, `Power2`, `ApparentPower1`, `ApparentPower2`, `ReactivePower1`, `ReactivePower2`, `Factor1`, `Factor2`, `Voltage`, `Current1`, `Current2`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    data = (TIME, DN, DID, POW1, POW2, SW1, SW2, PowerUsedTotal, PowerUsedToday, Period1, Period2, Power1, Power2, ApparentPower1, ApparentPower2, ReactivePower1, ReactivePower2, Factor1, Factor2, Voltage, Current1, Current2)
                    
                    cursor.execute(sql, data)
                    db.commit()
                    #print("DUALR3")
                    
                    #retransmitting formatted data
                    retransmitJSONData = {
                        "Switch1": SW1,
                        "Switch2": SW2,
                        "Power1": POW1,
                        "Power2": POW2,
                        "Factor1": Factor1,
                        "Factor2": Factor2,
                        "Period1": Period1,
                        "Period2": Period2,
                        "Voltage": Voltage,
                        "Current1": Current1,
                        "Current2": Current2,
                        "ApparentPower1": ApparentPower1,
                        "ApparentPower2": ApparentPower2,
                        "PowerUsedToday": PowerUsedToday,
                        "PowerUsedTotal": PowerUsedTotal,
                        "ReactivePower1": ReactivePower1,
                        "ReactivePower2": ReactivePower2
                    }
                    
                    retransmitJSON = {
                    
                        "Time": TIME, 
                        "DeviceID": DID, 
                        "DeviceName": DN, 
                        "DeviceType": "DUALR3", 
                        "Data": retransmitJSONData
                    }
                    sendFormattedData ("data/"+Topics[1], retransmitJSON)
                    print(retransmitJSON)
                    return
                    
        #Sonoff POW316
        if "POW316" in Topics[1]:
            if "STATE" in Topics[2]:
                SonoffPOW316STATETemp = {
                    "MsgTopic":msg.topic[:-6], 
                    "ArrivalTime": DataJSON["Time"], 
                    "Message": DataJSON
                }
                return
            
            if "SENSOR" in Topics[2]:
                if (SonoffPOW316STATETemp["MsgTopic"] == msg.topic[:-7]) and (SonoffPOW316STATETemp["ArrivalTime"] == DataJSON["Time"]):
                    TIME = DataJSON["Time"]
                    DN = msg.topic[5:-7]
                    DID = DN[-6:]
                    if SonoffPOW316STATETemp["Message"]["POWER"] == "ON":
                        POW = 1
                    else:
                        POW = 0
                    
                    PowerUsedTotal = DataJSON["ENERGY"]["Total"]	
                    PowerUsedToday = DataJSON["ENERGY"]["Today"]
                    Period = DataJSON["ENERGY"]["Period"]
                    Power = DataJSON["ENERGY"]["Power"]
                    ApparentPower = DataJSON["ENERGY"]["ApparentPower"]
                    ReactivePower = DataJSON["ENERGY"]["ApparentPower"]
                    Factor = DataJSON["ENERGY"]["Factor"]
                    Voltage = DataJSON["ENERGY"]["Voltage"]
                    Current = DataJSON["ENERGY"]["Current"]

                    sql = "INSERT INTO `SONOFF_POW316_STATUS` (`ID`, `Time`, `DeviceName`, `DeviceID`, `POW`, `PowerUsedTotal`, `PowerUsedToday`, `Period`, `Power`, `ApparentPower`, `ReactivePower`, `Factor`, `Voltage`, `Current`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    data = (TIME, DN, DID, POW, PowerUsedTotal, PowerUsedToday, Period, Power, ApparentPower, ReactivePower, Factor, Voltage, Current)
                    
                    cursor.execute(sql, data)
                    db.commit()
                    
                    #retransmitting formatted data
                    retransmitJSONData = {
                        "Power": POW,
                        "Factor": Factor,
                        "Period": Period,
                        "Voltage": Voltage,
                        "Current": Current,
                        "ApparentPower": ApparentPower,
                        "PowerUsedToday": PowerUsedToday,
                        "PowerUsedTotal": PowerUsedTotal,
                        "ReactivePower": ReactivePower
                    }
                    
                    retransmitJSON = {
                    
                        "Time": TIME, 
                        "DeviceID": DID, 
                        "DeviceName": DN, 
                        "DeviceType": "POW316", 
                        "Data": retransmitJSONData
                    }
                    
                    sendFormattedData ("data/"+Topics[1], retransmitJSON)
                    
                    return

    elif (Topics[0] == "stat"):
        evalStatusMessages(Topics, DataJSON, msg.topic)
    
    
def registerDevice(DeviceID, DeviceType, DeviceName):
    global cursor
    global db
    
    
    #check if registered
    SQL_Check = "SELECT COUNT(*) FROM ALL_DEVICE_LIST WHERE DeviceID = '{0}'".format(DeviceID)
#    print(SQL_Check)
    cursor.execute(SQL_Check)
    isRegistered = cursor.fetchone()[0]
    
    
    #print(isRegistered)
    
    if (isRegistered == 0):
        print("Device not registered")
        #register device
        #Find template
        SQL_Template = "SELECT Template FROM DEVICE_TEMPLATES WHERE DeviceType = '{0}'".format(DeviceType)
        cursor.execute(SQL_Template)
        newTemplate = cursor.fetchone()[0]
#        print("Existing Template Found:" + newTemplate)
        
        SQL_Register = "INSERT INTO `ALL_DEVICE_LIST` (`ID`, `DeviceID`, `DeviceName`, `DeviceType`, `Variables`) VALUES (NULL, '{0}', '{1}', '{2}', '{3}');".format(DeviceID, DeviceName, DeviceType, newTemplate)
        cursor.execute(SQL_Register)
        db.commit()
        
    
def evalStatusMessages(Topics, DataJSON, topic):
    today = datetime.now()

    GetOnlineRecord = "SELECT * FROM `ONLINE_DEVICE_LIST` WHERE DeviceID = '{0}'".format(Topics[1][-6:])
    cursor.execute(GetOnlineRecord)
    existingRecord = cursor.fetchone()
    
    if (existingRecord == None):
        rec = list(OnlineDeviceTemplate)
        
        rec[0] = Topics[1][-6:]
        
        rec[2] = Topics[1]
        rec[3] = rec[2].split("_")[1]
        rec[4] = DataTableNames[rec[3]]
        rec[5] = "cmnd/" + rec[2]
    else:
        rec = list(existingRecord)
    if (Topics[2] == "STATUS5"):
        rec[1] = today.isoformat()
        rec[6] = DataJSON["StatusNET"]["Hostname"]		
        rec[7] = DataJSON["StatusNET"]["IPAddress"]		
        rec[8] = DataJSON["StatusNET"]["Gateway"]
        rec[9] = DataJSON["StatusNET"]["Subnetmask"]		
        #rec[10] = DataJSON["StatusNET"]["DNSServer1"]		
        rec[11] = DataJSON["StatusNET"]["Mac"]

    if (Topics[2] == "STATUS2"):
        rec[1] = today.isoformat()
        rec[12] = DataJSON["StatusFWR"]["Version"]		
        rec[13] = DataJSON["StatusFWR"]["Hardware"]	

    if (Topics[2] == "STATUS1"):
        rec[1] = today.isoformat()
        rec[14] = DataJSON["StatusPRM"]["GroupTopic"]		
        rec[15] = DataJSON["StatusPRM"]["StartupUTC"]		
        rec[16] = DataJSON["StatusPRM"]["RestartReason"]
        

    StatusUpdate = "REPLACE INTO `ONLINE_DEVICE_LIST` (`DeviceID`, `Time`, `DeviceName`, `DeviceType`, `DataTableName`, `ControlTopic`, `Hostname`, `IPAddress`, `Gateway`, `SubnetMask`, `DNSServer`, `MAC`, `FWVersion`, `Hardware`, `GroupTopic`, `Uptime`, `RestartReason`) VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}', '{16}');".format(rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6], rec[7], rec[8], rec[9], rec[10], rec[11], rec[12], rec[13], rec[14], rec[15], rec[16])
    cursor.execute(StatusUpdate)
    db.commit()
        
    
def DeviceUpdateSQL():
    global myhost, myuser, mypasswd, mydb
    db2 = MySQLdb.connect(host=myhost,user=myuser,passwd=mypasswd,db=mydb)   
    cursor2 = db2.cursor()
    
    while True:
        print("DELETING")
        DeviceUpdate = "DELETE FROM ONLINE_DEVICE_LIST WHERE Time < (NOW() - INTERVAL 5 MINUTE);"
        cursor2.execute(DeviceUpdate)
        db2.commit()
        time.sleep(60)
    
    
def background_device_monitor():
    time.sleep(3)
    while True:
        
        #DeleteOfflineDevices = "DELETE FROM ONLINE_DEVICE_LIST WHERE Time < (NOW() - INTERVAL 5 MINUTE);"
        #cursor.execute(DeleteOfflineDevices)
        #db.commit()
    
        client.publish("cmnd/tasmotas/STATUS", 1) 
        client.publish("cmnd/DHTs/STATUS", 1)    
        time.sleep(4)        
        
        client.publish("cmnd/tasmotas/STATUS", 2) 
        client.publish("cmnd/DHTs/STATUS", 2) 
        time.sleep(4)
        
        client.publish("cmnd/tasmotas/STATUS", 5) 
        client.publish("cmnd/DHTs/STATUS", 5) 
        
        time.sleep(52)
        

def on_disconnect():
    print("odpojene")

def background_thread():
    db = MySQLdb.connect(host=myhost,user=myuser,passwd=mypasswd,db=mydb)    
	
    count = 0    

    client.connect("192.168.1.63", 1884)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.loop_forever()

    time.sleep(1)


if __name__ == "__main__":
    BackgroundThread = threading.Thread(target=background_thread, name='Background')
    BackgroundThread.start()
    MonitoringThread = threading.Thread(target=background_device_monitor, name='Monitor')
    MonitoringThread.start()
    DeviceUpdateThread = threading.Thread(target=DeviceUpdateSQL, name='DeviceUpdate')
    DeviceUpdateThread.start()

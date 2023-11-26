from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
from logmodule import logModule
import json
import sys
class mongodb: 
  def getconnection(self):
   logmod= logModule()
   try: 
    #conn = MongoClient("mongodb://localhost:27017/") 
    conn = MongoClient("mongodb://nqdba:DurgA*#376@0.0.0.0:27017/admin")
    #conn = MongoClient()
    logmod.logmsg("Mongo db Connected successfully!!!","debug") 
    return conn 
   except: 
    logmod.logmsg("Error while connecting to MongoDB","error")   
     
  
  # database name: mydatabase 
  
  
  # Created or Switched to collection names: myTable 
  #collection = db.tbl_icon_devicelog 
  
  def insertlog(self,record):
     logmod= logModule()
     sys.setrecursionlimit(500)
     conn = self.getconnection()
     try: 
       conn.icon_db.tbl_icon_devcicelog.insert_one(json.loads(record))
       logmod.logmsg("mongo db success","mode")
       conn.close()
     except: 
       logmod.logmsg("Error in insert log MongoDB","error")
       conn.close()
     
  def insertbufferlog(self,record):
    logmod= logModule()
    sys.setrecursionlimit(500)
    db = self.getconnection()
    
    try: 
      db.tbl_icon_bufferdata.insert_one(json.loads(record))
      #db.close()
    except: 
     logmod.logmsg("Error in insert buffer log MongoDB","error") 
     #db.close()

from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
from logmodulecsv import logModule
import json
import sys
from pymongo.errors import BulkWriteError
class mongodb: 
  def getconnection(self):
   logmod= logModule()
   try: 
    #conn = MongoClient("localhost",27017,maxPoolSize=200)
    #conn = MongoClient("mongodb://localhost:27017/?maxPoolSize=200")
    conn = MongoClient("mongodb://nqdba:DurgA*#376@13.126.247.173:27017/admin?maxPoolSize=200")
    #conn = MongoClient("mongodb://nqdba:DurgA*#376@0.0.0.0:27017/admin")    
    #conn = MongoClient()
   
    logmod.logmsg("Mongo","Mongo db Connected successfully!!!","debug") 
    return conn
   except: 
    logmod.logmsg("Exception","Error while connecting to MongoDB","error")   
     
  
  # database name: mydatabase 
  
  
  # Created or Switched to collection names: myTable 
  #collection = db.tbl_icon_devicelog 
  
  def insertlog(self,record,mongoconnection):
    logmod= logModule()
    sys.setrecursionlimit(500)
    if(mongoconnection==None):
      conn= self.getconnection()
      #config.set('mongodb-config', 'mongoconnection', conn)
    else:
      conn= mongoconnection    
    
    db=conn.icon_db
    for n in record:
      try:
        db.tbl_icon_devcicelog.insert_one(n)
        #db.tbl_icon_devcicelog.insert_many(record)
        logmod.logmsg("mongo data inserted","","error") 
    
        #conn.close()
      except:
         logmod.logmsg("mongo",str(sys.exc_info()[0]),"error")
      #except BulkWriteError as bwe:
      #logmod.logmsg("exception in bulk insert",str(bwe.details['writeErrors']),"error") 
      #db.errlog.insert(json.loads(bwe.details['writeErrors']))
      #print(bwe.details['writeErrors'])
    return conn    
  def insertbufferlog(self,record,mongoconnection):
    logmod= logModule()
    sys.setrecursionlimit(500)
    if(mongoconnection==None):
      conn= self.getconnection()
      #config.set('mongodb-config', 'mongoconnection', conn)
    else:
      conn= mongoconnection  
    
    try:
      db=conn.icon_db    
      db.tbl_icon_bufferdata.insert_one(json.loads(record))
      #db.close()
    except: 
     logmod.logmsg("Error mongo",str(sys.exc_info()[0]),"error") 
     #db.close()
    return mongoconnection

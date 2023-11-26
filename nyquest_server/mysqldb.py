import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
from mysql.connector.connection import MySQLConnection
from logmodulecsv import logModule
from apifunctions import ApiFunctions
import configparser
import os,sys
config = configparser.ConfigParser()  
thisfolder = os.path.dirname(os.path.abspath(__file__))
configFilePath = os.path.join(thisfolder, 'serverconfig.config')
config.read(configFilePath)
dbhost=config.get('mysqldb-config', 'host')
dbname=config.get('mysqldb-config', 'database')
dbuser=config.get('mysqldb-config', 'user')
dbpasword=config.get('mysqldb-config', 'password')
dbport=config.get('mysqldb-config', 'port')
servertype=config.get('server-config', 'servertype')
connection_pool = None

class mysqldb:
 def getsqlconnection(self):
   logmod= logModule()
   global connection_pool
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport
   try: 
    
     connection_pool = pooling.MySQLConnectionPool(pool_name="pynative_pool",
                                                                  pool_size=32,
                                                                  pool_reset_session=True,
                                                                  host=dbhost,
                                                                  database=dbname,
                                                                  user=dbuser,
                                                                  password=dbpasword,port=dbport)
     #connection =connection_pool.get_connection()
      
     logmod.logmsg("sql connection","MySQL db Connected successfully!!!","debug") 
     return connection_pool
   except: 
     logmod.logmsg("Exception","Error while connecting to MySQL "+str(sys.exc_info()[0]),"error")
  
 def insertSummaryLog(self,jsondata,logtime,devid,num_samples,algostatusflag,daystatusflag,sol_energy,logtimehour): 
   global connection_pool
   if(connection_pool==None or connection_pool.pool_size==0):
       connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   logmod= logModule()
   updatetime=0
   try: 
     #logmod.logmsg("dbhost="+dbhost+",user="+dbuser+",dbname="+dbname+",dbpasword="+dbpasword+",dbport="+dbport,"debug")
     #logmod.logmsg(" logtimehour = " +str(logtimehour),"debug")
     
     if connection.is_connected():
      custid=0
      locid=0 
      dupicatecnt=0      
      cursor = connection.cursor(buffered=True)
      sqlduplicate="select count(*) cnt from tbl_icon_summary where dev_id='"+devid+"' and date_format(logtime,'%Y-%m-%d %H')= '"+logtimehour+"'"
      cursor.execute(sqlduplicate)
      row = cursor.fetchone()
      dupicatecnt=row[0]
      logmod.logmsg(devid," dupicatecnt = " +str(dupicatecnt),"debug")
      if(dupicatecnt==0): 
        sqlcust="select cust_id,location_id from tbl_icon_depl where dev_id='"+devid+"' and b_active=1"
        cursor.execute(sqlcust)
        row = cursor.fetchone()
        if(cursor.rowcount>0):
          custid=row[0]
          locid=row[1]
        
        
        dayflag_recorded=0
        query="select count(*) cnt from tbl_icon_summary where dev_id='"+devid+"' "
        cursor.execute(query)
        cntr = cursor.fetchone()[0]
        if(cntr>0):
          query="select daystart_flag from tbl_icon_summary where dev_id='"+devid+"' order by logtime desc limit 1"
          cursor.execute(query)
          dayflag_recorded = cursor.fetchone()[0]
        
        query="insert into tbl_icon_summary(dev_id,logtime,logdata,num_samples,algo_status_flag,daystart_flag,sum_sol_p,cust_id,location_id) values ('"+devid+"','"+logtime+"','"+jsondata+"',"+str(num_samples)+","+str(algostatusflag)+","+str(daystatusflag)+","+str(sol_energy)+","+str(custid)+","+str(locid)+")"
        logmod.logmsg("insert query="+query,'debug')
        cursor.execute(query)
        connection.commit()
        #for updating  stateduration daystart
        #apifn=ApiFunctions()
        #apifn.apigetcalls('updatestateduration_hour/'+devid)
        '''
        if(daystatusflag==1):
          
          if(dayflag_recorded==0):
            updatequery="update tbl_icon_device set last_log_date='"+logtime+"', daystart=date_format('"+logtime+"','%H:%m:%s') where dev_id='"+devid+"'"
            cursor.execute(updatequery)
            updatetime=1
        if(updatetime==0):
          updatequery="update tbl_icon_device set last_log_date='"+logtime+"' where dev_id='"+devid+"'"
          cursor.execute(updatequery)
        
        
        '''
        
        logmod.logmsg(devid," Log summary Record inserted successfully","debug")
        cursor.close()
   except :
        #logmod.logmsg(devid,"Error while connecting to insertSummaryLog MySQL ","error")
        logmod.logmsg(devid,str(sys.exc_info()[0]),"error")
        connection.rollback()
   finally:
        if (connection.is_connected()):
         #cursor.close()
         connection.close()
         connection=None
        #for updating  stateduration daystart
        apifn=ApiFunctions()
        apifn.apigetcalls('updatestateduration_hour/'+devid) 
        
        
 
 def insertUseableSOC(self,voltdata,currentdata,socdata,deviceid,logtime):
   
   global connection_pool
   logmod= logModule()
   logmod.logmsg(deviceid,"inside insertUseableSOC logtime="+logtime,"debug")
   if(connection_pool==None):
       connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():
       
        cursor = connection.cursor(buffered=True)        
        cursor.execute("select a.id,b.cust_id,b.location_id from tbl_icon_device a left join tbl_icon_depl b on a.dev_id=b.dev_id  where a.dev_id='"+deviceid+"' and b.b_active=1")
        #devid = cursor.fetchone()[0]
        row = cursor.fetchone()
        if(cursor.rowcount>0):
          devid =row[0]
          custid=row[1]
          locid=row[2]
        i=0
        for n in voltdata:
           nvolt=voltdata[i]
           ncurrent=currentdata[i]
           nsoc=socdata[i]
           query="insert into tbl_icon_useableSOC_log(device_id,logdate,n_voltage,n_current,n_soc,cust_id,location_id) values ('"+str(devid)+"','"+logtime+"','"+str(nvolt)+"',"+str(ncurrent)+","+str(nsoc)+","+str(custid)+","+str(locid)+")"
           
           cursor.execute(query)
           i+=1
           
        connection.commit()
        logmod.logmsg(deviceid, " Record inserted successfully","debug")
        cursor.close()
   except :
     #logmod.logmsg(deviceid,"Error while connecting to insertUseableSOC MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
 def insertReadVal(self,deviceid,data,addloc):
   
   global connection_pool
   logmod= logModule()
   logmod.logmsg(deviceid,"inside insertReadVal","debug")
   if(connection_pool==None):
       connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():
       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select a.id,b.cust_id,b.location_id from tbl_icon_device a left join tbl_icon_depl b on a.dev_id=b.dev_id  where a.dev_id='"+deviceid+"' and b.b_active=1")
        #devid = cursor.fetchone()[0]
        row = cursor.fetchone()
        if(cursor.rowcount>0):
          devid =row[0]
          custid=row[1]
          locid=row[2]
        cursor.execute("select parameter_id from tbl_icon_threshold_parameter where addressloc='"+addloc+"'")
        parid = cursor.fetchone()[0]
        
        query="select count(*) from tbl_icon_device_threshold where device_id='"+str(devid)+"' and parameter_id="+str(parid)+" and cust_id="+str(custid)+" and location_id="+str(locid) 
        cursor.execute(query)
        cnt= cursor.fetchone()[0]
        if(cnt==0): 
           query="INSERT INTO tbl_icon_device_threshold (device_id,parameter_id,n_value,cust_id,location_id) values ('"+str(devid)+"','"+str(parid)+"','"+str(data)+"',"+str(custid)+","+str(locid)+")"
        else:
           query="update tbl_icon_device_threshold set n_value='"+str(data)+"' where device_id='"+str(devid)+"' and parameter_id="+str(parid)+" and cust_id="+str(custid)+" and location_id="+str(locid)
        cursor.execute(query)
        connection.commit()
        logmod.logmsg(deviceid,"insertReadVal successfully","debug")
        cursor.close()
   except :
     #logmod.logmsg(deviceid,"insertReadVal Error while connecting to MySQL","error")     
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
 def getWriteCount(self,deviceid):
   global connection_pool
   logmod= logModule()
   logmod.logmsg("getWriteCount ,"+deviceid,"started","error")
   if(connection_pool==None):
    connection_pool=self.getsqlconnection()

   connection=connection_pool.get_connection()
   logmod.logmsg("getWriteCount ,"+deviceid,"before function start","error")
   try:
      
      if connection.is_connected(): 
        logmod.logmsg("getWriteCount ,"+deviceid,"after db connection","error")      
        cursor = connection.cursor(buffered=True)
        cursor.execute("select count(1) from tbl_icon_device a inner join tbl_icon_threshold_log b on a.id=b.device_id where b_updated=0 and a.dev_id='"+deviceid+"'")
        cnt = cursor.fetchone()
        writecnt=cnt[0]
        logmod.logmsg("getWriteCount ,"+deviceid,"got writecnt","error") 
        cursor.execute("select count(1) from tbl_icon_device a inner join tbl_icon_device_stateassert b on a.id=b.device_id where b_updated=0 and a.dev_id='"+deviceid+"'")
        cnt1 = cursor.fetchone()
        statecnt=cnt1[0]
        logmod.logmsg("getWriteCount ,"+deviceid,"got statecnt","error")
        cursor.execute("select count(1) from tbl_icon_device a inner join tbl_icon_device_reset b on a.id=b.device_id where b_reset=0 and a.dev_id='"+deviceid+"'")
        cnt2 = cursor.fetchone()
        resetcnt=cnt2[0]
        logmod.logmsg("getWriteCount ,"+deviceid,"got resetcnt","error")
        cursor.close()
        
        return writecnt,statecnt,resetcnt
   except :
     #logmod.logmsg("getWriteCount Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     return 0,0,0
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
        
 def getWriteData(self,deviceid):
   
   global connection_pool
   logmod= logModule()
   if(connection_pool==None):
       connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select addressloc,n_value,noofbytes,vdatatype from tbl_icon_device a inner join tbl_icon_threshold_log b on a.id=b.device_id inner join tbl_icon_threshold_parameter p on b.parameter_id=p.parameter_id where b_updated=0 and a.dev_id='"+deviceid+"' order by b.dtcreated limit 1")
        row = cursor.fetchone()
        
        addloc=row[0]
        ndata=row[1]
        noofbytes=row[2]
        datatype=row[3]
        cursor.close()
        logmod.logmsg(deviceid,"write data="+str(ndata)+" addloc="+addloc,'debug')
        return noofbytes,datatype,addloc,ndata
   except :
     #logmod.logmsg("getWriteData Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     return '',0
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
 def updateOTAStatus(self,deviceid):      
    pass
 def updateWriteStatus(self,deviceid,addloc):
   
   global connection_pool
   logmod= logModule()
   devcat='L'
   if(servertype=='HV'):
      devcat='H'
   if(connection_pool==None):
    connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select id,fwversion from tbl_icon_device where dev_id='"+deviceid+"'")
        row = cursor.fetchone()
        devid = row[0]
        fwversion=row[1]
        cursor.execute("select parameter_id from tbl_icon_threshold_parameter where addressloc='"+addloc+"' and fwversion='"+str(fwversion)+"' and dev_category='"+devcat+"'")
        parid = cursor.fetchone()[0]
        cursor.execute("select n_value,cust_id,location_id from tbl_icon_threshold_log where b_updated=0 and device_id ="+str(devid)+" and parameter_id="+str(parid))
        row = cursor.fetchone()
        nvalue = row[0]
        custid = row[1]
        locationid = row[2]
        cursor.execute("update tbl_icon_threshold_log set b_updated=1,dtupdated=now() where b_updated=0 and device_id ="+str(devid)+" and parameter_id="+str(parid))
        cursor.execute("select count(1) cnt from tbl_icon_device_threshold where device_id ="+str(devid)+" and parameter_id="+str(parid)+" and cust_id="+str(custid)+" and location_id="+str(locationid))
        cnt = cursor.fetchone()[0]
        
        if(cnt==0):
          cursor.execute("insert into tbl_icon_device_threshold (device_id,parameter_id,n_value,cust_id,location_id) values ("+str(devid)+","+str(parid)+","+str(nvalue)+","+str(custid)+","+str(locationid)+")")
        else:
          cursor.execute("update tbl_icon_device_threshold set n_value="+str(nvalue)+" where device_id ="+str(devid)+" and parameter_id="+str(parid))
        
        logmod.logmsg("","Updated write status for address ="+str(parid)+" and device ="+str(devid),"debug")
        connection.commit()        
        cursor.close()
       
   except Exception as ex:
      template = "An exception of type {0} occurred. Arguments:\n{1!r}"
      message = template.format(type(ex).__name__, ex.args)
      #logmod.logmsg("updateWriteStatus Error while connecting to MySQL updateWriteStatus ","error")
      logmod.logmsg(deviceid,str(message),"error")
      connection.rollback()
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
        
        
        
 # state change 
 def getStatechangeCount(self,deviceid):
   
   global connection_pool
   logmod= logModule()
   if(connection_pool==None):
    connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select count(*) from tbl_icon_device a inner join tbl_icon_device_stateassert b on a.id=b.device_id where b_updated=0 and a.dev_id='"+deviceid+"'")
        cnt = cursor.fetchone()
        cursor.close()
        return cnt[0]
   except:
     #logmod.logmsg("getStatechangeCount Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     return 0
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
        
 def getStagechangeData(self,deviceid):
   
   global connection_pool
   logmod= logModule()
   stateid=0
   starthour=0
   startminute=0
   duration=0
   datarow=[]
   asserttype=''
   if(connection_pool==None):
    connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select state_id,n_hour,n_minute,n_duration,asserttype,log_id from tbl_icon_device a inner join tbl_icon_device_stateassert b on a.id=b.device_id where b_updated=0 and a.dev_id='"+deviceid+"' order by b.dtcreated limit 1")
        row = cursor.fetchone()
        asserttype=str(row[4])
        logid=row[5]
        if(asserttype=='statechange'):
          datarow.append(row[0])
          datarow.append(row[1])
          datarow.append(row[2])
          datarow.append(row[3])
          stateid=row[0]
        elif(asserttype=='ftexittime'):          
          datarow.append(row[1])
          datarow.append(row[2])
        else:  
          cursor.execute("select n_value from tbl_icon_device_stateassert_parameter b inner join tbl_icon_threshold_parameter a on a.parameter_id=b.parameter_id where log_id="+str(logid)+ " order by a.parameter_id")
          data = cursor.fetchall()
          i=0
          for row in data :
             datarow.append(row[0])
             i+=1
        cursor.close()
        logmod.logmsg("statechange data="+str(stateid)+" starthour="+str(starthour),'debug')
        return asserttype,datarow
   except:
     #logmod.logmsg("getStagechangeData Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,"Exception in getStagechangeData "+str(sys.exc_info()[0]),"error")
     return '',datarow
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
 def updateStatechangeStatus(self,deviceid,asserttype):
   
   global connection_pool
   logmod= logModule()
   if(connection_pool==None):
    connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select id from tbl_icon_device where dev_id='"+deviceid+"'")
        devid = cursor.fetchone()[0]
        
        cursor.execute("update tbl_icon_device_stateassert set b_updated=1 where device_id ="+str(devid)+" and b_updated=0 and asserttype='"+asserttype+"'")
        connection.commit()
        logmod.logmsg(deviceid,"Updated state status for device ="+str(devid),"debug")
        cursor.close()
       
   except:
     #logmod.logmsg("in updateStatechangeStatus Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None       
 def getOTACount(self,deviceid):
    if(deviceid=='A12XA1000008'):
        return 0
    else:
     return 0  
     
 def getResetCount(self,deviceid):
   
   global connection_pool
   logmod= logModule()
   if(connection_pool==None):
    connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select count(*) from tbl_icon_device a inner join tbl_icon_device_reset b on a.id=b.device_id where b_reset=0 and a.dev_id='"+deviceid+"'")
        cnt = cursor.fetchone()
        cursor.close()
        return cnt[0]
   except:
     #logmod.logmsg("getResetCount Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     return 0
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None       
        
 def updateResetStatus(self,deviceid,flagtype):
   
   global connection_pool
   logmod= logModule()
   if(connection_pool==None):
       connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select id from tbl_icon_device where dev_id='"+deviceid+"'")
        devid = cursor.fetchone()[0]
        
        cursor.execute("update tbl_icon_device_reset set b_reset=1,dtupdated=now() where device_id ="+str(devid)+" and b_reset=0 and flagtype='"+flagtype+"'")
        connection.commit()
        logmod.logmsg(deviceid,"Updated reset status for device ="+str(devid),"debug")
        cursor.close()
       
   except:
     #logmod.logmsg("updateResetStatus Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None

 def getResetData(self,deviceid):
   
   global connection_pool
   logmod= logModule()
   if(connection_pool==None):
       connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select flagtype from tbl_icon_device a inner join tbl_icon_device_reset b on a.id=b.device_id where b_reset=0 and a.dev_id='"+deviceid+"' order by b.dtcreated limit 1")
        row = cursor.fetchone()
        
        flagtype=row[0]
        
        cursor.close()
        logmod.logmsg(deviceid,"getResetData data="+flagtype,'debug')
        return flagtype
   except:
     #logmod.logmsg("getResetData Error while connecting to MySQL","error")
     logmod.logmsg(deviceid,str(sys.exc_info()[0]),"error")
     return '',0
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None
 def insertAlertlog(self,alertid,devid,logdate):
    #params='{"deviceid":"'+devid+'","alertid":"'+alertid+'","logdate":"'+logdate+'"}'
    #--edited on 16-Nov-2022--#
    params='{"deviceid":"'+devid+'","alertid":"'+str(alertid)+'","logdate":"'+logdate+'"}'
    apifn=ApiFunctions()
    apifn.apipostcalls('alertlog',params)
    
 def getDeviceForWrite(self):
   global connection_pool
   logmod= logModule()
   devids=''
   if(connection_pool==None):
       connection_pool=self.getsqlconnection()
   connection=connection_pool.get_connection()
   try:
      
      if connection.is_connected():       
        cursor = connection.cursor(buffered=True)
        cursor.execute("select dev_id from tbl_icon_device where id in (select device_id from tbl_icon_threshold_log where b_updated=0) or id in (select device_id from tbl_icon_device_stateassert where b_updated=0) or id in (select device_id from tbl_icon_device_reset where b_reset=0)")
        rows = cursor.fetchall()
        for dev in rows:
          if(devids==''):
            devids=dev[0]
          else:            
           devids=devids+','+dev[0]
        
        cursor.close()
       
        return devids
   except Exception as ex:        
          template = "An exception of type {0} occurred. Arguments:\n{1!r}"
          message = template.format(type(ex).__name__, ex.args)         
          logmod.logmsg(devids,str(message),"error")
     
          return ''
   finally:
     if (connection.is_connected()):
        #cursor.close()
        connection.close()
        connection=None

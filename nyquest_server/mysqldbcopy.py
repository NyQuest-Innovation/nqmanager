import mysql.connector
from mysql.connector import Error
from logmodule import logModule
from apifunctions import ApiFunctions
import configparser
import os
config = configparser.ConfigParser()  
thisfolder = os.path.dirname(os.path.abspath(__file__))
configFilePath = os.path.join(thisfolder, 'serverconfig.config')
config.read(configFilePath)
dbhost=config.get('mysqldb-config', 'host')
dbname=config.get('mysqldb-config', 'database')
dbuser=config.get('mysqldb-config', 'user')
dbpasword=config.get('mysqldb-config', 'password')
dbport=config.get('mysqldb-config', 'port')
connection = mysql.connector.connect()
class mysqldb:
 def insertSummaryLog(self,jsondata,logtime,devid,num_samples,algostatusflag,daystatusflag,sol_energy,logtimehour): 
   
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
   updatetime=0
   try: 
     #logmod.logmsg("dbhost="+dbhost+",user="+dbuser+",dbname="+dbname+",dbpasword="+dbpasword+",dbport="+dbport,"debug")
     logmod.logmsg(" logtimehour = " +str(logtimehour),"debug")
     connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
     if connection.is_connected():
      custid=0
      locid=0 
      dupicatecnt=0      
      cursor = connection.cursor()
      sqlduplicate="select count(*) cnt from tbl_icon_summary where dev_id='"+devid+"' and date_format(logtime,'%Y-%m-%d %H')= '"+logtimehour+"'"
      cursor.execute(sqlduplicate)
      row = cursor.fetchone()
      dupicatecnt=row[0]
      logmod.logmsg("devid="+devid+" dupicatecnt = " +str(dupicatecnt),"debug")
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
        apifn=ApiFunctions()
        apifn.apigetcalls('updatestateduration_hour/'+devid)
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
        
        logmod.logmsg(str(cursor.rowcount)+ " Log summary Record inserted successfully","debug")
        cursor.close()
   except Error as e:
        logmod.logmsg("Error while connecting to insertSummaryLog MySQL ","error")
        logmod.logmsg(e,"error")
        connection.rollback()
   finally:
        if (connection.is_connected()):
         cursor.close()
         connection.close()
        
        
 
 def insertUseableSOC(self,voltdata,currentdata,socdata,deviceid,logtime):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
   logmod.logmsg("inside insertUseableSOC devid="+deviceid+" logtime="+logtime,"debug")
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():
       
        cursor = connection.cursor()
        cursor.execute("select id from tbl_icon_device where dev_id='"+deviceid+"'")
        devid = cursor.fetchone()[0]
        i=0
        for n in voltdata:
           nvolt=voltdata[i]
           ncurrent=currentdata[i]
           nsoc=socdata[i]
           query="insert into tbl_icon_useableSOC_log(device_id,logdate,n_voltage,n_current,n_soc) values ('"+str(devid)+"','"+logtime+"','"+str(nvolt)+"',"+str(ncurrent)+","+str(nsoc)+")"
           
           cursor.execute(query)
           i+=1
           
        connection.commit()
        logmod.logmsg(str(cursor.rowcount)+ " Record inserted successfully","debug")
        cursor.close()
   except Error as e:
     logmod.logmsg("Error while connecting to insertUseableSOC MySQL","error")
     logmod.logmsg(e,"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
        
 def insertReadVal(self,deviceid,data,addloc):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
   logmod.logmsg("inside insertReadVal devid="+deviceid,"debug")
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():
       
        cursor = connection.cursor()
        cursor.execute("select id from tbl_icon_device where dev_id='"+deviceid+"'")
        devid = cursor.fetchone()[0]
        cursor.execute("select parameter_id from tbl_icon_threshold_parameter where addressloc='"+addloc+"'")
        parid = cursor.fetchone()[0]
        
        query="select count(*) from tbl_icon_device_threshold where device_id='"+str(devid)+"' and parameter_id="+str(parid) 
        cursor.execute(query)
        cnt= cursor.fetchone()[0]
        if(cnt==0): 
           query="INSERT INTO tbl_icon_device_threshold (device_id,parameter_id,n_value) values ('"+str(devid)+"','"+str(parid)+"','"+str(data)+"')"
        else:
           query="update tbl_icon_device_threshold set n_value='"+str(data)+"' where device_id='"+str(devid)+"' and parameter_id="+str(parid)
        cursor.execute(query)
        connection.commit()
        logmod.logmsg("insertReadVal successfully","debug")
        cursor.close()
   except Error as e:
     logmod.logmsg("insertReadVal Error while connecting to MySQL","error")     
     logmod.logmsg(e,"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
 
 def getWriteCount(self,deviceid):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select count(*) from tbl_icon_device a inner join tbl_icon_threshold_log b on a.id=b.device_id where b_updated=0 and a.dev_id='"+deviceid+"'")
        cnt = cursor.fetchone()
        cursor.close()
        return cnt[0]
   except Error as e:
     logmod.logmsg("getWriteCount Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     return 0
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
        
        
 def getWriteData(self,deviceid):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select addressloc,n_value,noofbytes,vdatatype from tbl_icon_device a inner join tbl_icon_threshold_log b on a.id=b.device_id inner join tbl_icon_threshold_parameter p on b.parameter_id=p.parameter_id where b_updated=0 and a.dev_id='"+deviceid+"' order by b.dtcreated limit 1")
        row = cursor.fetchone()
        
        addloc=row[0]
        ndata=row[1]
        noofbytes=row[2]
        datatype=row[3]
        cursor.close()
        logmod.logmsg("write data="+str(ndata)+" addloc="+addloc,'debug')
        return noofbytes,datatype,addloc,ndata
   except Error as e:
     logmod.logmsg("getWriteData Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     return '',0
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
        
 def updateOTAStatus(self,deviceid):      
    pass
 def updateWriteStatus(self,deviceid,addloc):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select id from tbl_icon_device where dev_id='"+deviceid+"'")
        devid = cursor.fetchone()[0]
        cursor.execute("select parameter_id from tbl_icon_threshold_parameter where addressloc='"+addloc+"'")
        parid = cursor.fetchone()[0]
        cursor.execute("select n_value from tbl_icon_threshold_log where b_updated=0 and device_id ="+str(devid)+" and parameter_id="+str(parid))
        nvalue = cursor.fetchone()[0]
        cursor.execute("update tbl_icon_threshold_log set b_updated=1,dtupdated=now() where device_id ="+str(devid)+" and parameter_id="+str(parid))
        cursor.execute("select count(1) from tbl_icon_device_threshold where device_id ="+str(devid)+" and parameter_id="+str(parid))
        cnt = cursor.fetchone()[0]
        if(cnt==0):
          cursor.execute("insert into tbl_icon_device_threshold (device_id,parameter_id,n_value) values ("+str(devid)+","+str(parid)+","+str(nvalue)+")")
        else:
          cursor.execute("update tbl_icon_device_threshold set n_value="+str(nvalue)+" where device_id ="+str(devid)+" and parameter_id="+str(parid))
        connection.commit()
        logmod.logmsg("Updated write status for address ="+str(parid)+" and device ="+str(devid),"debug")
        #cursor.close()
       
   except Error as e:
     logmod.logmsg("updateWriteStatus Error while connecting to MySQL updateWriteStatus ","error")
     logmod.logmsg(e,"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
        
        
        
        
 # state change 
 def getStatechangeCount(self,deviceid):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select count(*) from tbl_icon_device a inner join tbl_icon_device_stateassert b on a.id=b.device_id where b_updated=0 and a.dev_id='"+deviceid+"'")
        cnt = cursor.fetchone()
        cursor.close()
        return cnt[0]
   except Error as e:
     logmod.logmsg("getStatechangeCount Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     return 0
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
        
        
 def getStagechangeData(self,deviceid):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
   stateid=0
   starthour=0
   startminute=0
   duration=0
   datarow=[]
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select state_id,n_hour,n_minute,n_duration,asserttype,log_id from tbl_icon_device a inner join tbl_icon_device_stateassert b on a.id=b.device_id where b_updated=0 and a.dev_id='"+deviceid+"' order by b.dtcreated limit 1")
        row = cursor.fetchone()
        asserttype=row[4]
        logid=row[5]
        if(asserttype=='statechange'):
          datarow.append(row[0])
          datarow.append(row[1])
          datarow.append(row[2])
          datarow.append(row[3])
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
   except Error as e:
     logmod.logmsg("getStagechangeData Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     return '',0
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
      
 def updateStatechangeStatus(self,deviceid,asserttype):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select id from tbl_icon_device where dev_id='"+deviceid+"'")
        devid = cursor.fetchone()[0]
        
        cursor.execute("update tbl_icon_device_stateassert set b_updated=1 where device_id ="+str(devid)+" and b_updated=0 and asserttype='"+asserttype+"'")
        connection.commit()
        logmod.logmsg("Updated state status for device ="+str(devid),"debug")
        #cursor.close()
       
   except Error as e:
     logmod.logmsg("in updateStatechangeStatus Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
               
 def getOTACount(self,deviceid):
    if(deviceid=='A12XA1000008'):
        return 0
    else:
     return 0  
     
 def getResetCount(self,deviceid):
   global dbhost
   global dbname
   global dbuser   
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select count(*) from tbl_icon_device a inner join tbl_icon_device_reset b on a.id=b.device_id where b_reset=0 and a.dev_id='"+deviceid+"'")
        cnt = cursor.fetchone()
        #cursor.close()
        return cnt[0]
   except Error as e:
     logmod.logmsg("getResetCount Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     return 0
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
               
        
 def updateResetStatus(self,deviceid,flagtype):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select id from tbl_icon_device where dev_id='"+deviceid+"'")
        devid = cursor.fetchone()[0]
        
        cursor.execute("update tbl_icon_device_reset set b_reset=1,dtupdated=now() where device_id ="+str(devid)+" and b_reset=0 and flagtype='"+flagtype+"'")
        connection.commit()
        logmod.logmsg("Updated reset status for device ="+str(devid),"debug")
        cursor.close()
       
   except Error as e:
     logmod.logmsg("updateResetStatus Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     connection.rollback()
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
       

 def getResetData(self,deviceid):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
  
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select flagtype from tbl_icon_device a inner join tbl_icon_device_reset b on a.id=b.device_id where b_reset=0 and a.dev_id='"+deviceid+"' order by b.dtcreated limit 1")
        row = cursor.fetchone()
        
        flagtype=row[0]
        
        cursor.close()
        logmod.logmsg("getResetData data="+flagtype,'debug')
        return flagtype
   except Error as e:
     logmod.logmsg("getResetData Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
     return '',0
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
 def updateStateDuration(self,deviceid,timestamp,stateid,yearid,monid,dayid,hrid,logtype,resetflag):
   global dbhost
   global dbname
   global dbuser
   global dbpasword
   global dbport   
   
   global connection
   logmod= logModule()
   sec=1
   try:
      connection = mysql.connector.connect(host=dbhost,
                                         database=dbname,
                                         user=dbuser,
                                         password=dbpasword,port=dbport)
      if(logtype==3):
         sec=30
      if connection.is_connected():       
        cursor = connection.cursor()
        cursor.execute("select count(*) cnt from tbl_icon_state_duration where dev_id='"+deviceid+"' and year_id="+str(yearid)+" and MONTH_ID="+str(monid)+" and day_id="+str(dayid)+" and hour_id="+str(hrid)+" and state_id="+str(stateid))
        cnt = cursor.fetchone()[0]
        if(cnt>0):
          cursor.execute("update tbl_icon_state_duration set n_duration=n_duration+"+str(sec)+" where dev_id='"+deviceid+"' and year_id="+str(yearid)+" and MONTH_ID="+str(monid)+" and day_id="+str(dayid)+" and hour_id="+str(hrid)+" and state_id="+str(stateid))
        
          
        else:
          sql="insert into tbl_icon_state_duration (year_id,month_id,day_id,hour_id,dev_id,state_id,n_duration,dt_date) values("+str(yearid)+","+str(monid)+","+str(dayid)+","+str(hrid)+",'"+deviceid+"',"+str(stateid)+","+str(sec)+",date_format('"+timestamp+"','%Y-%m-%d %H:00:00'))" 
          cursor.execute(sql)
        
        updatequery="update tbl_icon_device set last_log_date='"+timestamp+"' where dev_id='"+deviceid+"'"
        if(resetflag==1):
           updatequery="update tbl_icon_device set last_log_date='"+timestamp+"',daystart=date_format('"+timestamp+"','%H:%m:%s') where dev_id='"+deviceid+"'"
        cursor.execute(updatequery)
        connection.commit()
        logmod.logmsg("Updated updateStateDuration status for device ="+deviceid,"debug")
        #cursor.close()
       
   except Error as e:
     connection.rollback()
     logmod.logmsg("updateStateDuration Error while connecting to MySQL","error")
     logmod.logmsg(e,"error")
   finally:
     if (connection.is_connected()):
        cursor.close()
        connection.close()
import socket,struct,sys
import math
from ctypes import *
import binascii
import json
from logmodule import logModule
from serverclass import auth_req_payload
from serverclass import auth_req_tag
from serverclass import auth_res_payload
from serverclass import auth_res_tag
from serverclass import log_rec_tag
from datetime import datetime
from serverfunctions import ServerFunctions
from mongodb import mongodb
from mysqldb import mysqldb
import configparser
import os
config = configparser.ConfigParser()  
thisfolder = os.path.dirname(os.path.abspath(__file__))
configFilePath = os.path.join(thisfolder, 'serverconfig.config')

config.read(configFilePath)
addcommonflag=config.get('server-config', 'commonflag_address')
calibrationsyncword=int(config.get('server-config', 'calibrationsyncword'),0)
class DataFunctions:
 logmod= logModule()
 
 autagin=auth_req_tag()
 
 def requesthandler(self,buff,sessionid,ghashclient):
   
   logmod = logModule()
   servfn=ServerFunctions()
   logmod.logmsg("inside requesthandler sesionid="+str(sessionid),'debug')
   aureqtag= auth_req_tag()
   aupayload= auth_req_payload()
   autagin=auth_req_tag()
   autagin.syncword[0]=buff[0]
   autagin.syncword[1]=buff[1]
   autagin.syncword[2]=buff[2]
   autagin.req_id=buff[3]
   autagin.sessionid=(buff[7]<<24)|(buff[6]<<16)|(buff[5]<<8)|buff[4]
   autagin.seqid=buff[8]
   autagin.proto_major_ver=buff[9]
   autagin.proto_minor_ver=buff[10]
   autagin.payload_len=(buff[12]<<8)|buff[11]
   dpayload=bytes(servfn.dcrypt(bytearray(buff[13:34]),autagin.payload_len))
   aupayload=auth_req_payload()
   aupayload.device_id[0]=dpayload[0]
   aupayload.device_id[1]=dpayload[1]
   aupayload.device_id[2]=dpayload[2]
   aupayload.device_id[3]=dpayload[3]
   aupayload.device_id[4]=dpayload[4]
   aupayload.device_id[5]=dpayload[5]
   aupayload.device_id[6]=dpayload[6]
   aupayload.device_id[7]=dpayload[7]
   aupayload.device_id[8]=dpayload[8]
   aupayload.device_id[9]=dpayload[9]
   aupayload.device_id[10]=dpayload[10]
   aupayload.device_id[11]=dpayload[11]
   aupayload.dev_type=dpayload[12]
   aupayload.hw_version=(dpayload[14]<<8)|dpayload[13]
   aupayload.fw_version=(dpayload[16]<<8)|dpayload[15]
   aupayload.challenge=(dpayload[20]<<24)|(dpayload[19]<<16)|(dpayload[18]<<8)|dpayload[17]
   autagin.payload=dpayload
   devid=str(aupayload.device_id,'utf-8')
   sesid=sessionid
   logmod.logmsg("devid="+devid,"debug")
          
   now = datetime.now()  
   dt_string = now.strftime("%d%m%Y%H%M%S")
   time_stamp = (c_uint8 * 6)(*range(6))
          
   time_stamp[0] = (c_uint8)(int(now.strftime("%d")))
   time_stamp[1] = (c_uint8)(int(now.strftime("%m")))
   time_stamp[2] = (c_uint8)(int(now.strftime("%y")))
   time_stamp[3] = (c_uint8)(int(now.strftime("%H")))
   time_stamp[4] = (c_uint8)(int(now.strftime("%M")))
   time_stamp[5] = (c_uint8)(int(now.strftime("%S")))
   
   autagpayload = auth_res_payload()
   autagpayload.seq_id=aureqtag.seqid
   autagpayload.time_stamp=time_stamp
   autagpayload.sesn_id=sesid
   autagpayload.challenge_res=aupayload.challenge
   autagpayload.res_flags=1
                            
   challenge=aupayload.challenge
   s1= sesid & 0xFF
   s2 = sesid>>8 & 0xFF
   s3 = sesid>>16 & 0xFF
   s4 = sesid >>24 & 0xFF
              
   y1= challenge & 0xFF
   y2 = challenge>>8 & 0xFF
   y3 = challenge>>16 & 0xFF
   y4 = challenge >>24 & 0xFF
   r1 = autagpayload.res_flags & 0xFF
   r2 = (autagpayload.res_flags>>8) & 0xFF
   mydb=mysqldb()
   writemode='false'
   statemode='false'
   resetmode='false'
   otamode='false'
   if (mydb.getOTACount(devid)>0):
       otamode='true'
       logmod.logmsg(" devid="+devid+" otamode="+otamode,'debug')
   if (mydb.getResetCount(devid)>0):
       resetmode='true'
       logmod.logmsg(" devid="+devid+" resetmode="+resetmode,'debug')
   if(resetmode=='false'):
     if (mydb.getStatechangeCount(devid)>0):
       statemode='true'
       logmod.logmsg("devid="+devid+ " statemode="+statemode,'debug')
   if(resetmode=='false' and statemode=='false'):
     if (mydb.getWriteCount(devid)>0):
       writemode='true'
       logmod.logmsg("devid="+devid+" writemode="+writemode,'debug')   
   
   
   for x in ghashclient['client']:    
     if (x['devid']==devid and x['readmode']=='true') :
         logmod.logmsg("configuraion read mode devid="+devid,"debug")
         r1=9
         r2=0
         #print("readmode for devid ",devid)
         
   if (writemode=='true'):
         logmod.logmsg("configuraion write mode devid="+devid,"debug")
         r1=9
         r2=0
   if (statemode=='true') :
         logmod.logmsg("configuraion statemode devid="+devid,"debug")
         r1=9
         r2=0
   if (resetmode=='true') :
         logmod.logmsg("configuraion statemode devid="+devid,"debug")
         r1=9
         r2=0     
   if (otamode=='true') :
         logmod.logmsg("configuraion otamode devid="+devid,"debug")
         r1=9
         r2=0
   buffpayload=(c_uint8 * 17)(*range(17))
   buffpayload[0]=aureqtag.seqid
   buffpayload[1]=time_stamp[0]
   buffpayload[2]=time_stamp[1]
   buffpayload[3]=time_stamp[2]
   buffpayload[4]=time_stamp[3]
   buffpayload[5]=time_stamp[4]
   buffpayload[6]=time_stamp[5]
   buffpayload[7]=s1
   buffpayload[8]=s2
   buffpayload[9]=s3
   buffpayload[10]=s4
   buffpayload[11]=y1
   buffpayload[12]=y2
   buffpayload[13]=y3
   buffpayload[14]=y4
   buffpayload[15]=r1
   buffpayload[16]=r2
   strpl=''
   for a in buffpayload:
     strpl=strpl+','+str(a)
     
   logmod.logmsg(strpl,'debug')
   
   payload=servfn.ncrypt(buffpayload,len(buffpayload))
   
   buffrestag=(c_uint8 * 25)(*range(25))
   buffrestag[0]=0x55
   buffrestag[1]=0xAA
   buffrestag[2]=0x55
   buffrestag[3]=0x01
   buffrestag[4]=0x11
   buffrestag[5]=0x00
   buffrestag[6]=payload[0]
   buffrestag[7]=payload[1]
   buffrestag[8]=payload[2]
   buffrestag[9]=payload[3]
   buffrestag[10]=payload[4]
   buffrestag[11]=payload[5]
   buffrestag[12]=payload[6]
   buffrestag[13]=payload[7]
   buffrestag[14]=payload[8]
   buffrestag[15]=payload[9]
   buffrestag[16]=payload[10]
   buffrestag[17]=payload[11]
   buffrestag[18]=payload[12]
   buffrestag[19]=payload[13]
   buffrestag[20]=payload[14]
   buffrestag[21]=payload[15]
   buffrestag[22]=payload[16]
   
   chksum=servfn.crc16(buffrestag[0:23])
   buffrestag[24] = (chksum>>8) & 0xFF
   buffrestag[23] = chksum & 0xFF
   
   return otamode,resetmode,writemode,statemode,devid,buffrestag,challenge
 def readhandler(self,sesionid,readcnt):
   logmod = logModule()
   servfn=ServerFunctions()
    
   logmod.logmsg("inside readhandler readcnt="+str(readcnt),'debug')
   readres = (c_uint8 * 13)(*range(13))
   readrespayload=(c_uint8 * 3)(*range(3))
   payloadlen=3
   reqid=6
   noofbytes=52
   s1 = sesionid & 0xFF
   s2 = sesionid>>8 & 0xFF
   s3 = sesionid>>16 & 0xFF
   s4 = sesionid >>24 & 0xFF
   readres[0]=calibrationsyncword
   readres[1]=reqid
   readres[2]=s1
   readres[3]=s2
   readres[4]=s3
   readres[5]=s4 
   readres[6]=payloadlen & 0xFF 
   readres[7]=(payloadlen>>8) & 0xFF
   
   if(readcnt==0):
     readres[8]=0x81
     readres[9]=0xFE
     readres[10]=80
   elif(readcnt==1):
     readres[8]=0xD1
     readres[9]=0XFE
     readres[10]=80
   elif(readcnt==2):
     readres[8]=0x21
     readres[9]=0XFF
     readres[10]=80
   elif(readcnt==3):
     readres[8]=0x71
     readres[9]=0XFF
     readres[10]=5
   elif(readcnt==4):
     readres[8]=0x1A
     readres[9]=0XFE
     readres[10]=4
   elif(readcnt==5):
     readres[8]=0x1F
     readres[9]=0XFE
     readres[10]=4
   
   chksum=servfn.crc16(readres[0:11])
   readres[12] = (chksum>>8) & 0xFF
   readres[11] = chksum & 0xFF
   
   return readres
 
 def readdatahandler(self,buff,deviceid):
   logmod = logModule()
   servfn=ServerFunctions()
   voltarray=[] #(float * 20)(*range(20))
   currentarray=[] #(float * 20)(*range(20))
   socarray=[] #(float * 20)(*range(20))
   totlen=len(buff)
   lastdataidx=totlen-1 #checksum 2 and -1 for array
   logmod.logmsg("inside readdatahandler deviceid="+deviceid,'debug')
   dpayload=buff    #[8:lastdataidx]
   minidx=0
   maxidx=minidx+4
   strpayload=""
   for t in dpayload:
       strpayload=strpayload+","+str(t)
   logmod.logmsg("read data dpayload="+strpayload,'debug')
   for idx in range(0,20,1): 
         
      arr=struct.unpack("<f", bytes(dpayload[minidx:maxidx])) 
      socvalue=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
      logmod.logmsg("readdata socvalue="+str(socvalue),'debug') 
      socarray.append(socvalue) 
      minidx=maxidx
      maxidx=minidx+4    
   for idx in range(0,20,1):   
      arr=struct.unpack("<f", bytes(dpayload[minidx:maxidx])) 
      voltvalue=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
      voltarray.append(voltvalue)
      minidx=maxidx
      maxidx=minidx+4
   
   for idx in range(0,20,1):   
      arr=struct.unpack("<f", bytes(dpayload[minidx:maxidx])) 
      curvalue=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
      currentarray.append(curvalue) 
      minidx=maxidx
      maxidx=minidx+4  
   
   tlen=len(dpayload)
   tstart=tlen-5
   tarr= dpayload[tstart:tlen]  
   now = datetime.now() 
   time_stamp = (c_uint8 * 6)(*range(6))
   time_stamp[0]=c_long(tarr[0]).value
   time_stamp[1]=c_long(tarr[1]).value
   time_stamp[2]=c_long(tarr[2]).value
   time_stamp[3]=c_long(tarr[3]).value
   time_stamp[4]=c_long(tarr[4]).value
   time_stamp[5]=(c_uint8)(int(now.strftime("%S")))
   #logmod.logmsg("timeStamp year="+ str(time_stamp[2])+" month="+ str(time_stamp[1])+ " day="+str(time_stamp[0]),'debug')
   logtime=str(time_stamp[2]+2000)+'-'+str(time_stamp[1]).zfill(2)+'-'+str(time_stamp[0]).zfill(2)+' '+str(time_stamp[3]).zfill(2)+':'+str(time_stamp[4]).zfill(2)+':'+str(time_stamp[5]).zfill(2)
  
   mydb=mysqldb()
   mydb.insertUseableSOC(voltarray,currentarray,socarray,deviceid,logtime)
 
 def readdatasoc(self,devid,buff): 
   logmod = logModule()
   mydb=mysqldb()
   arr=struct.unpack("<f", bytes(buff[0:4])) 
   data=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   mydb.insertReadVal(devid,data,'FE1A')
 def readdatavftexit(self,devid,buff): 
   logmod = logModule()
   mydb=mysqldb()
   arr=struct.unpack("<f", bytes(buff[0:4])) 
   data=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   mydb.insertReadVal(devid,data,'FE1F')  
 def writeUpdatehandler(self,devid,addloc): 
   logmod = logModule()
   mydb=mysqldb()
   mydb.updateWriteStatus(devid,addloc)
 def UpdateOTAStatus(self,devid): 
   logmod = logModule()
   mydb=mysqldb()
   mydb.updateOTAStatus(devid) 
 def otahandler(self,devid,sesionid,dataindex,fwstartaddress,filename,buff):
    logmod = logModule()
    logmod.logmsg("inside otahandler devid="+devid+" dataindex="+str(dataindex)+" fwstartaddress="+str(fwstartaddress),'debug')
    with open(filename, "rb") as f:
      rawcode = f.read(1024)
      idx=0
      if(dataindex==0):
        if(buff[1]!=12):
         #print(dataindex)       
         sentmsg=self.processotadata(rawcode,fwstartaddress,sesionid)
        else:
         if(self.checkprevdata(rawcode,fwstartaddress,sesionid,buff)==True):
            sentmsg=self.processotadata(rawcode,fwstartaddress+1024,sesionid)
            dataindex=dataindex+1
            fwstartaddress=fwstartaddress+1024
         else:
            sentmsg=self.processotadata(rawcode,fwstartaddress,sesionid)
         
        return sentmsg,dataindex,fwstartaddress
      else:
         while rawcode:   
            if(idx>0 and  dataindex==idx):            
                      
              if(self.checkprevdata(rawcode,fwstartaddress,sesionid,buff)==True):
                sentmsg=self.processotadata(rawcode,fwstartaddress+1024,sesionid)
                fwstartaddress=fwstartaddress+1024
                dataindex=dataindex+1
                if(len(rawcode)<1024): 
                  dataindex=-1
                  logmod.logmsg("inside otahandler read completed devid="+devid ,'debug')
              else:
                sentmsg=self.processotadata(rawcode,fwstartaddress,sesionid)
              
              return sentmsg,dataindex,fwstartaddress
            rawcode = f.read(1024)
            idx+=1
 def checkprevdata(self,rawcode,fwstartaddress,sessionid,buff):
     servfn=ServerFunctions()
     logmod = logModule()
     payload=buff[8:1038]
     logmod.logmsg("inside checkprevdata payload length="+str(len(payload)),'debug')
     
     dedata=bytes(servfn.dcrypt(bytearray(payload),len(payload)))
     #dedata=payload
     if len(rawcode)<1024:
        rawcode=rawcode.ljust(1024,b'\0')
        
     data =dedata[6:1030]
     '''
     if(fwstartaddress==0x20000):
       strbuff=""
       for a in data:
        strbuff=strbuff+","+str(a) 
       logmod.logmsg("rec_data first packet="+strbuff,'debug')
     strbuff=""
     for a in rawcode:
        strbuff=strbuff+","+str(a) 
     #logmod.logmsg("rawcode="+strbuff,'debug')
     '''
     matching=True
     for i in range(0,len(rawcode)):
         if(rawcode[i]!=data[i]):
             matching=False
             break
     logmod.logmsg("inside checkprevdata payload i="+str(i)+" matching="+str(matching),'debug')
     return matching
         
 def processotadata(self,rawcode,fwstartaddress,sessionid):
     arrlen=len(rawcode)
     servfn=ServerFunctions()
     logmod = logModule()
     if len(rawcode)<1024:
        rawcode=rawcode.ljust(1024,b'\0')
        rawcode=rawcode.ljust(1024,b'\0')
     #sub_array1 = rawcode.ljust(1024,b'\0')
     
     datalen=1024
     paclen=1024+6  
          
     #payload=[] 
     #checksumpkt=[]  
     checksumpkt=(c_uint8 * 1037)(*range(1037))
     payload=(c_uint8 * 1030)(*range(1030))
     #endata=(c_uint8 * 1030)(*range(1030))
     tx_buffer=(c_uint8 * 1040)(*range(1040))     
               
     checksumpkt[0]=0x0C #1 byte
     checksumpkt[1]=sessionid & 0xFF
     checksumpkt[2] = sessionid>>8 & 0xFF
     checksumpkt[3] = sessionid>>16 & 0xFF
     checksumpkt[4] = sessionid >>24 & 0xFF #4 bytes
     checksumpkt[5]= paclen & 0xFF
     checksumpkt[6]= paclen>>8 & 0xFF
     #fwadd=int(str(fwstartaddress),16)
     fwadd=fwstartaddress
     payload[0]= fwadd & 0xFF
     payload[1]= fwadd>>8 & 0xFF
     payload[2]= fwadd>>16 & 0xFF
     payload[3]= fwadd >>24 & 0xFF
     payload[4]= datalen & 0xFF
     payload[5]= (datalen & 0xFF00)>>8 #datalen>>8 & 0xFF
     idx=6
     for i in range(0,len(rawcode)):
       payload[idx]=rawcode[i]
       idx=idx+1
       
     endata=bytes(servfn.ncrypt(bytearray(payload[0:1030]),1030))
     '''
     if(fwstartaddress==0x20000):
      strbuff=""
      for a in endata:
        strbuff=strbuff+","+str(a) 
      logmod.logmsg("inside processotadata endata="+strbuff,'debug')
     ''' 
     checksumpkt[7:1037]=endata[0:1030]
     #checksumpkt[7:1037]=payload[0:1030]
     
     logmod.logmsg("inside processotadata checksumpkt length="+str(len(checksumpkt)),'debug')
     
     chksum=servfn.crc16(checksumpkt)
     #chksum=0
     tx_buffer[0]=calibrationsyncword
     tx_buffer[1:1038]=checksumpkt[0:1037]
     tx_buffer[1038]= chksum & 0xFF
     tx_buffer[1039] =(chksum>>8) & 0xFF
     
     return tx_buffer
 def writehandler(self,devid,sesionid):
   logmod = logModule()
   servfn=ServerFunctions()
   mydb=mysqldb()
   reqid=5
   noofbytes,datatype,addloc,data=mydb.getWriteData(devid)
   logmod.logmsg("inside writehandler devid="+devid+" data="+str(data)+" addloc="+addloc,'debug')
   s1= sesionid & 0xFF
   s2 = sesionid>>8 & 0xFF
   s3 = sesionid>>16 & 0xFF
   s4 = sesionid >>24 & 0xFF
   addloc1='0x'+addloc
   loc=int(addloc1, 16)
   if(noofbytes==1):
     readres = (c_uint8 * 14)(*range(14))
     
     payloadlen=4
     readres[0]=calibrationsyncword
     readres[1]=reqid
     readres[2]=s1
     readres[3]=s2
     readres[4]=s3
     readres[5]=s4 
     readres[6]=payloadlen & 0xFF 
     readres[7]=(payloadlen>>8) & 0xFF
     
     readres[8]=loc & 0xFF
     readres[9]=(loc>>8) & 0xFF 
     readres[10]=1
     intdata=int(data)
     readres[11]=intdata 
     
     chksum=servfn.crc16(readres[0:12])
     readres[12] = (chksum>>8) & 0xFF
     readres[13] = chksum & 0xFF
   elif(noofbytes==2):
     readres = (c_uint8 * 15)(*range(15))
     
     payloadlen=5
     readres[0]=calibrationsyncword
     readres[1]=reqid
     readres[2]=s1
     readres[3]=s2
     readres[4]=s3
     readres[5]=s4 
     readres[6]=payloadlen & 0xFF 
     readres[7]=(payloadlen>>8) & 0xFF
     
     readres[8]=loc & 0xFF
     readres[9]=(loc>>8) & 0xFF 
     readres[10]=2
     intdata=int(data)
     readres[11]=intdata & 0xFF 
     readres[12]=(intdata>>8) & 0xFF
     chksum=servfn.crc16(readres[0:13])
     readres[13] = (chksum>>8) & 0xFF
     readres[14] = chksum & 0xFF
   elif(noofbytes==34): #serverconfig
     readres = (c_uint8 * 47)(*range(47))   
     payloadlen=37
     readres[0]=calibrationsyncword
     readres[1]=reqid
     readres[2]=s1
     readres[3]=s2
     readres[4]=s3
     readres[5]=s4
     readres[6]=payloadlen & 0xFF 
     readres[7]=(payloadlen>>8) & 0xFF
     readres[8]=loc & 0xFF
     readres[9]=(loc>>8) & 0xFF
     readres[10]=34
     serverip=config.get('server-config', 'newserverip')
     serverport=config.get('server-config', 'newserverport')
     iparray1 = serverip.ljust(32,'\0')
     iparray1 = iparray1.ljust(32,'\0')
     intdata = int(serverport)

     tx_buffer=(binascii.hexlify(bytearray(iparray1,'utf-8')))
     tx_buff = [int(tx_buffer[i:i+2], 16) for i in range(0, len(tx_buffer), 2)]
     readres[11:43]=tx_buff[0:32]
     readres[43]=intdata & 0xFF
     readres[44]=(intdata>>8) & 0xFF 
     chksum=servfn.crc16(readres[0:45])
     readres[45] = (chksum>>8) & 0xFF
     readres[46] = chksum & 0xFF
   else:
     readres = (c_uint8 * 17)(*range(17))
     
     payloadlen=7
     readres[0]=calibrationsyncword
     readres[1]=reqid
     readres[2]=s1
     readres[3]=s2
     readres[4]=s3
     readres[5]=s4 
     readres[6]=payloadlen & 0xFF 
     readres[7]=(payloadlen>>8) & 0xFF
     
     readres[8]=loc & 0xFF
     readres[9]=(loc>>8) & 0xFF 
     readres[10]=4
   
     tx_buff =list(struct.pack("!f", data))   
     readres[11]=tx_buff[3]
     readres[12]=tx_buff[2]
     readres[13]=tx_buff[1]
     readres[14]=tx_buff[0]
   
     chksum=servfn.crc16(readres[0:15])
     readres[15] = (chksum>>8) & 0xFF
     readres[16] = chksum & 0xFF
   
   return readres,addloc
   
 def statechangehandler(self,devid,sesionid):
   logmod = logModule()
   servfn=ServerFunctions()
   addstatechange=config.get('server-config', 'statechange_address')
   reqid=5
   
   s1= sesionid & 0xFF
   s2 = sesionid>>8 & 0xFF
   s3 = sesionid>>16 & 0xFF
   s4 = sesionid >>24 & 0xFF
   
   mydb=mysqldb()   
   asserttype,datarow=mydb.getStagechangeData(devid)
   logmod.logmsg("inside statechangehandler devid="+devid+" asserttype="+asserttype,'debug')
   if(asserttype=='statechange'):
      payloadlen=8
      loc=int(addstatechange, 16)
      readres = (c_uint8 * 18)(*range(18))
      readres[0]=calibrationsyncword
      readres[1]=reqid
      readres[2]=s1
      readres[3]=s2
      readres[4]=s3
      readres[5]=s4
      readres[6]=payloadlen & 0xFF 
      readres[7]=(payloadlen>>8) & 0xFF
   
      readres[8]=loc & 0xFF
      readres[9]=(loc>>8) & 0xFF 
      readres[10]=5
      readres[11]=datarow[0] #stateid
      readres[12]=datarow[1] #hour
      readres[13]=datarow[2] #minute
      readres[14]=datarow[3] & 0xFF #duration
      readres[15]=(datarow[3]>>8) & 0xFF #duration
  
      chksum=servfn.crc16(readres[0:16])
      readres[16] = (chksum>>8) & 0xFF
      readres[17] = chksum & 0xFF
   elif(asserttype=='ftexittime'):
      payloadlen=5
      addftexittime=config.get('server-config', 'ftexittime_address')
      loc=int(addftexittime, 16)
      readres = (c_uint8 * 15)(*range(15))
      readres[0]=calibrationsyncword
      readres[1]=reqid
      readres[2]=s1
      readres[3]=s2
      readres[4]=s3
      readres[5]=s4
      readres[6]=payloadlen & 0xFF 
      readres[7]=(payloadlen>>8) & 0xFF
   
      readres[8]=loc & 0xFF
      readres[9]=(loc>>8) & 0xFF 
      readres[10]=2
      readres[11]=datarow[0] #hour
      readres[12]=datarow[1] #minute
      chksum=servfn.crc16(readres[0:13])
      readres[13] = (chksum>>8) & 0xFF
      readres[14] = chksum & 0xFF
   else:
      if(asserttype=='solarenergy'):
        addloc=config.get('server-config', 'solarenergy_address')
        loc=int(addloc, 16)
        readres = (c_uint8 * 21)(*range(21))
      elif(asserttype=='loadbucket'):
        addloc=config.get('server-config', 'loadbucket_address')
        loc=int(addloc, 16)
        readres = (c_uint8 * 69)(*range(69))
      else:
        addloc=config.get('server-config', 'solarbucket_address')
        loc=int(addloc, 16)
        readres = (c_uint8 * 37)(*range(37))
        
      payloadlen=(len(datarow)*4) +3
      
      readres[0]=calibrationsyncword
      readres[1]=reqid
      readres[2]=s1
      readres[3]=s2
      readres[4]=s3
      readres[5]=s4      
      readres[6]=payloadlen & 0xFF 
      readres[7]=(payloadlen>>8) & 0xFF
   
      readres[8]=loc & 0xFF
      readres[9]=(loc>>8) & 0xFF 
      readres[10]=len(datarow)*4
      idx=10
      datalen=len(datarow)
      for data in datarow:
        tx_buff =list(struct.pack("!f", data))   
        readres[idx+1]=tx_buff[3]
        readres[idx+2]=tx_buff[2]
        readres[idx+3]=tx_buff[1]
        readres[idx+4]=tx_buff[0]
        idx=idx+4
      
      lastidx=8+payloadlen
      #logmod.logmsg("state devid="+devid+" payloadlen="+str(payloadlen)+" lastidx="+str(lastidx)+" datalen="+str(datalen),'debug')
      chksum=servfn.crc16(readres[0:lastidx])
      readres[lastidx] = (chksum>>8) & 0xFF
      readres[lastidx+1] = chksum & 0xFF
      
   return readres,asserttype
 def resethandler(self,devid,sesionid):
   logmod = logModule()
   servfn=ServerFunctions()
   mydb=mysqldb()
   logmod.logmsg("inside resethandler devid="+devid,'debug')
   
   readres = (c_uint8 * 15)(*range(15))
   
   payloadlen=5
   reqid=5
   flagtype=mydb.getResetData(devid)   
   s1= sesionid & 0xFF
   s2 = sesionid>>8 & 0xFF
   s3 = sesionid>>16 & 0xFF
   s4 = sesionid >>24 & 0xFF
   readres[0]=calibrationsyncword
   readres[1]=reqid
   readres[2]=s1
   readres[3]=s2
   readres[4]=s3
   readres[5]=s4 
   readres[6]=payloadlen & 0xFF 
   readres[7]=(payloadlen>>8) & 0xFF
   loc=int(addcommonflag, 16)
   readres[8]=loc & 0xFF
   readres[9]=(loc>>8) & 0xFF 
   readres[10]=2
   
   data=0
   if(flagtype=='reset' ):
     data=servfn.Bit(9)
    
   elif(flagtype=='exittime'):
     data=servfn.Bit(1)
   elif(flagtype=='soccalc'):
     data=servfn.Bit(2)
   elif(flagtype=='repeatinit'):
     data=servfn.Bit(3)
   elif(flagtype=='socerror'):
     data=servfn.Bit(4)
   elif(flagtype=='battemp'):
     data=servfn.Bit(5)
     
   elif(flagtype=='absorption'):
     data=servfn.Bit(11)
   elif(flagtype=='equalization'):
     data=servfn.Bit(12)
   data1= (data>>8)
   
   readres[11]=data&0xFF
   readres[12]=data1&0xFF
   
   chksum=servfn.crc16(readres[0:13])
   readres[13] = (chksum>>8) & 0xFF
   readres[14] = chksum & 0xFF
   
   return readres,flagtype
 def resetUpdatehandler(self,devid,flagtype): 
   logmod = logModule()
   mydb=mysqldb()
   mydb.updateResetStatus(devid,flagtype)  
 def commonflaghandler(self,devid,sesionid,asserttype):
   logmod = logModule()
   servfn=ServerFunctions()
    
   logmod.logmsg("inside commonflaghandler devid="+devid,'debug')
   readres = (c_uint8 * 15)(*range(15))
   
   payloadlen=5
   reqid=5
   commonbit=0
   if(asserttype=='ftexittime'):
     commonbit=1
   elif(asserttype=='solarenergy'):
     commonbit=6
   elif(asserttype=='loadbucket'):
     commonbit=8 
   elif(asserttype=='solarbucket'):
     commonbit=7
   elif(asserttype=='noofdays'):
     commonbit=10
   '''
   elif(asserttype=='absorption'):
     commonbit=11
   elif(asserttype=='equalization'):
     commonbit=12
   '''
   s1= sesionid & 0xFF
   s2 = sesionid>>8 & 0xFF
   s3 = sesionid>>16 & 0xFF
   s4 = sesionid >>24 & 0xFF
   readres[0]=calibrationsyncword
   readres[1]=reqid
   readres[2]=s1
   readres[3]=s2
   readres[4]=s3
   readres[5]=s4 
   readres[6]=payloadlen & 0xFF 
   readres[7]=(payloadlen>>8) & 0xFF
   loc=int(addcommonflag, 16)
   readres[8]=loc & 0xFF
   readres[9]=(loc>>8) & 0xFF 
   readres[10]=2
   data=servfn.Bit(commonbit)
   readres[11]=data&0xFF
   readres[12]=(data>>8)&0xFF
   
   chksum=servfn.crc16(readres[0:13])
   readres[13] = (chksum>>8) & 0xFF
   readres[14] = chksum & 0xFF
   
   return readres  
 def statechangeUpdatehandler(self,devid,asserttype): 
   logmod = logModule()
   mydb=mysqldb()
   mydb.updateStatechangeStatus(devid,asserttype)  
   
 def logrequesthandler(self,buff,ghashclient,chk_sum):
  autagin=auth_req_tag()
  logmod = logModule()
  mdb = mongodb()
  mydb=mysqldb()
  servfn=ServerFunctions()
  now = datetime.now()  
  readmode='false'
  writemode='false'
  statemode='false'
  #dt_string = now.strftime("%d%m%Y%H%M%S")
  dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
  time_stamp = (c_uint8 * 6)(*range(6))
  
  sessionid=(buff[7]<<24)|(buff[6]<<16)|(buff[5]<<8)|buff[4]
  lastchecksum=0
  for x in ghashclient['client']:
      if x['sessionid']==sessionid:                 
          devid=x['devid']
          lastchecksum=x['checksum'] 
  if(lastchecksum!=chk_sum):
   strbuff=""
   for a in buff:
     strbuff=strbuff+","+str(a)    
   
   logpayload=log_rec_tag()
   autagin.payload_len=(buff[12]<<8)|buff[11]
   
   dpayload=bytes(servfn.dcrypt(bytearray(buff[13:82]),autagin.payload_len))
   strpl=''
   for p in dpayload:
      strpl=strpl+','+str(p)
   logmod.logmsg("inside logrequesthandler devid ="+devid+" logdata payload="+strpl,'debug')
   
   #time_stamp[5]=(c_uint8)(int(now.strftime("%S"))) 
   time_stamp[0]=c_long(dpayload[0]).value
   time_stamp[1]=c_long(dpayload[1]).value
   time_stamp[2]=c_long(dpayload[2]).value
   time_stamp[3]=c_long(dpayload[3]).value
   time_stamp[4]=c_long(dpayload[4]).value
   time_stamp[5]=(c_uint8)(int(now.strftime("%S"))) 
  
   arr=struct.unpack("<f", bytes(dpayload[5:9])) 
   logpayload.bat_v=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0   
   arr=struct.unpack("<f", bytes(dpayload[9:13]))
   logpayload.bat_chg_i=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[13:17]))
   logpayload.bat_dis_i=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[17:21]))
   logpayload.sol_v=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[21:25]))
   logpayload.sol_i=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[25:29]))
   logpayload.ac_load_i=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0   
   logpayload.temp_adc=(dpayload[30]<<8)|dpayload[29]
   arr=struct.unpack("<f", bytes(dpayload[31:35]))
   logpayload.bat_soc=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0   
   logpayload.switch_stat=dpayload[35]
   arr=struct.unpack("<f", bytes(dpayload[36:40]))
   logpayload.sum_sol_e=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[40:44]))
   logpayload.sum_sol_bat_e=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[44:48]))
   logpayload.sum_bat_load_e=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[48:52]))
   logpayload.sum_ac_chg_e=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   arr=struct.unpack("<f", bytes(dpayload[52:56]))
   logpayload.sum_ac_load_e=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   logpayload.num_samples=(dpayload[57]<<8)|dpayload[56]
   logpayload.log_type=dpayload[58]
   logpayload.state_change_flag=dpayload[59]
   logpayload.dev_status_flag=(dpayload[61]<<8)|dpayload[60]
   logpayload.algorithm_status_flag=(dpayload[63]<<8)|dpayload[62]
   arr=struct.unpack("<f", bytes(dpayload[64:68]))
   logpayload.inverter_volt=round(float('.'.join(str(ele) for ele in arr)),2) if not(math.isnan(float('.'.join(str(ele) for ele in arr)))) else 0
   logpayload.error=dpayload[68]
   
   if (logpayload.dev_status_flag&(1<<13)):
       readmode='true'
       logmod.logmsg("device="+devid+" and readmode="+readmode,'debug')
    
    
   jsonbuffdata='{"dev_id":"'+devid+'","timestamp":"'+dt_string+'","buff":"'+strbuff+'"}'
   #logmod.logmsg("jsonbuffdata "+jsonbuffdata,'debug')
   #mdb.insertbufferlog(jsonbuffdata)    
   logtime=str(time_stamp[2]+2000)+'-'+str(time_stamp[1]).zfill(2)+'-'+str(time_stamp[0]).zfill(2)+' '+str(time_stamp[3]).zfill(2)+':'+str(time_stamp[4]).zfill(2)+':'+str(time_stamp[5]).zfill(2)
       
   stat=0
   if (logpayload.algorithm_status_flag&(1<<13)):     
     stat=1 
   resetflag=0
   if (logpayload.algorithm_status_flag&(1<<13)==0 and logpayload.algorithm_status_flag&(1<<11)==0):     
      resetflag=1
      
   #logmod.logmsg("inside log devid="+devid+" daystartflag="+str(stat),'debug')
   num_samples=logpayload.num_samples
   sol_energy = logpayload.sum_sol_e/3600
   chg_energy_ac = logpayload.sum_ac_chg_e/3600
   chg_energy = (logpayload.sum_sol_bat_e + logpayload.sum_ac_chg_e)/3600
   dis_energy = logpayload.sum_bat_load_e/3600
   #logmod.logmsg("logpayload.sum_sol_e="+str(logpayload.sum_sol_e),'debug')
   jsondata='{"dev_id":"'+devid+'","timestamp":"'+logtime+'","bat_v":'+str(round(logpayload.bat_v,2))+',"bat_chg_i":'+str(round(logpayload.bat_chg_i,2))
   jsondata=jsondata+',"bat_dis_i":'+str(round(logpayload.bat_dis_i,2))+',"sol_i":'+str(round(logpayload.sol_i,2))
   jsondata=jsondata+',"sum_chg_p":'+str(round(chg_energy,2))+',"sum_dis_p":'+str(round(dis_energy,2))+',"sol_v":'+str(round(logpayload.sol_v,2))+',"sum_sol_p":'+str(round(sol_energy,2))
   jsondata=jsondata+',"sum_ac_load_i":'+str(round(logpayload.ac_load_i,2))+',"temp_adc":'+str(round(logpayload.temp_adc,2))+',"bat_soc":'+str(round(logpayload.bat_soc,2))
   jsondata=jsondata+',"switch_stat":'+str(round(logpayload.switch_stat,2))+',"sum_sol_e":'+str(round(logpayload.sum_sol_e,2))+',"sum_sol_bat_e":'
   jsondata=jsondata+str(round(logpayload.sum_sol_bat_e/3600,2))+',"sum_bat_load_e":'+str(round(logpayload.sum_bat_load_e/3600,2))+',"sum_ac_chg_e":'+str(round(logpayload.sum_ac_chg_e/3600,2))
   jsondata=jsondata+',"sum_ac_chg_p":'+str(round(logpayload.sum_ac_load_e/3600,2))+',"num_samples":'+str(logpayload.num_samples)+',"log_type":'
   jsondata=jsondata+str(logpayload.log_type)+',"state_change_flag":'+str(logpayload.state_change_flag)
   jsondata=jsondata+',"dev_status_flag":'+str(logpayload.dev_status_flag)+',"algorithm_status_flag":'+str(logpayload.algorithm_status_flag)
   jsondata=jsondata+',"inverter_volt":'+str(round(logpayload.inverter_volt,2))+',"error":'+str(logpayload.error)+',"daystartflag":'+str(stat)+',"resetflag":'+str(resetflag)+'}'
   #logmod.logmsg("insertlog jsondata"+jsondata ,'debug')
   #try:
   mdb.insertlog(jsondata)
   #except:
   #logmod.logmsg("Exception in mongo insertlog" ,'error')
   if(logpayload.log_type=='1' or logpayload.log_type==1 ): 
        #try: 
        logtimehour=logtime=str(time_stamp[2]+2000)+'-'+str(time_stamp[1]).zfill(2)+'-'+str(time_stamp[0]).zfill(2)+' '+str(time_stamp[3]).zfill(2);
         
        mydb.insertSummaryLog(jsondata,logtime,devid,num_samples,logpayload.algorithm_status_flag,stat,sol_energy,logtimehour)
        #except:
        #logmod.logmsg("Exception in insertSummaryLog" ,'error')
   '''
   else:       
        debuglogtime=str(time_stamp[2]+2000)+'-'+str(time_stamp[1]).zfill(2)+'-'+str(time_stamp[0]).zfill(2)+' '+str(time_stamp[3]).zfill(2)+':'+str(time_stamp[4]).zfill(2)+':'+str(time_stamp[5]).zfill(2)
        dt=str(time_stamp[2]+2000)+'-'+str(time_stamp[1]).zfill(2)+'-'+str(time_stamp[0]).zfill(2)+' '+str(time_stamp[3]).zfill(2)+':'+str(time_stamp[4]).zfill(2)+':'+str(time_stamp[5]).zfill(2)
        try:
           
           mydb.updateStateDuration(devid,dt,logpayload.switch_stat,time_stamp[2]+2000,time_stamp[1],time_stamp[0],time_stamp[3],logpayload.log_type,resetflag)
           
        except:
           logmod.logmsg("Exception in mysql updateStateDuration" ,'error')
   '''            
   logpayload=(c_uint8 * 3)(*range(3))
   logrestag=(c_uint8 * 11)(*range(11))
   logrestag[0]=0x55
   logrestag[1]=0xAA
   logrestag[2]=0x55
   logrestag[3]=0x02
   logrestag[4]=3 & 0xFF
   logrestag[5]=0x00
   
   logpayload[0]=buff[8]
   dummy=1|(1<<15)
   logpayload[1]=dummy & 0xFF
   logpayload[2]=(dummy>>8)&0xFF
   
   payload=bytes(servfn.ncrypt(bytearray(logpayload[0:3]),3))
   logrestag[6]=payload[0]
   logrestag[7]=payload[1]  
   logrestag[8]=payload[2]
   
   
   chksum=servfn.crc16(logrestag[0:9])
   logrestag[9] = chksum & 0xFF
   logrestag[10] = (chksum>>8) & 0xFF
   
   return readmode,logrestag   
 def configExitOTA(self,devid,sesionid):
   logmod = logModule()
   servfn=ServerFunctions()
   logmod.logmsg("inside configExitOTA devid="+devid,'debug')
   
   '''
    0XCC 1byte,
    0x05 1 byte,
    Session Id 4 bytes, 
    3+n 2 bytes,
    EEPROM ADDR 2 bytes,
    DATA LEN 1 bytes, 
    FW ADDR 4 bytes, 
    CHECKSUM 2 bytes
   '''
   readres = (c_uint8 * 17)(*range(17))
   reqid=0x05
   CONFIG_FIRM_START_ADDR= 0xFF00
   fwstartaddress=0x20000
   s1= sesionid & 0xFF
   s2 = sesionid>>8 & 0xFF
   s3 = sesionid>>16 & 0xFF
   s4 = sesionid >>24 & 0xFF
   totlen=3+4
   datlen=4
   readres[0]=0XCC
   readres[1]=reqid
   readres[2]=s1
   readres[3]=s2
   readres[4]=s3
   readres[5]=s4 
   readres[6]=totlen & 0xFF
   readres[7]=totlen>>8 & 0xFF
   #intadd=int(str(CONFIG_FIRM_START_ADDR),16)
   intadd=CONFIG_FIRM_START_ADDR
   readres[8]=intadd & 0xFF
   readres[9]=intadd>>8 & 0xFF
   readres[10]=datlen
   #intfwadd=int(str(fwstartaddress),16)
   intfwadd=fwstartaddress
   readres[11]=intfwadd & 0xFF
   readres[12]=intfwadd>>8 & 0xFF
   readres[13]=intfwadd>>16 & 0xFF
   readres[14]=intfwadd >>24 & 0xFF
   
   chksum=servfn.crc16(readres[0:15])
   readres[15] = (chksum>>8) & 0xFF
   readres[16] = chksum & 0xFF
   return readres
 def configExit(self,devid,sesionid):
   logmod = logModule()
   servfn=ServerFunctions()
   logmod.logmsg("inside configExit devid="+devid,'debug')
   readres = (c_uint8 * 10)(*range(10))
   reqid=0x0A
      
   s1= sesionid & 0xFF
   s2 = sesionid>>8 & 0xFF
   s3 = sesionid>>16 & 0xFF
   s4 = sesionid >>24 & 0xFF
   readres[0]=calibrationsyncword
   readres[1]=reqid
   readres[2]=s1
   readres[3]=s2
   readres[4]=s3
   readres[5]=s4 
   readres[6]=0
   readres[7]=0
   
   
  
   chksum=servfn.crc16(readres[0:8])
   readres[8] = (chksum>>8) & 0xFF
   readres[9] = chksum & 0xFF
   
   return readres   
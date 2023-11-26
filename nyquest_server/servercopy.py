import socket,struct,sys,time
import concurrent.futures
import random
from concurrent.futures import ThreadPoolExecutor
from threading import *
from ctypes import *
import _thread
import uuid
import json
from logmodule import logModule
from serverclass import auth_req_payload
from serverclass import auth_req_tag
from serverclass import auth_res_payload
from serverclass import auth_res_tag
from serverclass import log_rec_tag
from serverclass import CommonMessageHeader
from datetime import datetime
from serverfunctions import ServerFunctions
from datafunctions import DataFunctions
import configparser
import os
import multiprocessing 
config = configparser.ConfigParser()  
thisfolder = os.path.dirname(os.path.abspath(__file__))
configFilePath = os.path.join(thisfolder, 'serverconfig.config') 
#configFilePath = r'serverconfig.config'
config.read(configFilePath)
calibrationsyncword=int(config.get('server-config', 'calibrationsyncword'),0)
logmodule = logModule()
datafn=DataFunctions()
servfn=ServerFunctions()
recordcnt=0
lastchksum=0
readbuff=bytearray()
challenge=0
addloc=''
flagtype=''
asserttype=''
gsessionid=0
def on_new_client(clientsocket,addr,cnt,sessionid):
    
    global devid
    global recordcnt
    global lastchksum
    global challenge
    global readcnt
    global addloc
    global flagtype
    global asserttype
    global readbuff
    global gsessionid
    global grmode
    global gsmode
    global gwmode
    global gremode
    global gomode
    
    if(cnt==1):
       recordcnt=servfn.generate_incrementer(recordcnt)
   
       
    configcnt=config.get('server-config', 'no-of-records')
    try:
        global greadarray
        global ghashclient
        logmodule.logmsg("inside on_new_client sessionid="+str(sessionid),'debug')
        aureqtag= auth_req_tag()
        aupayload= auth_req_payload()
        autagin=auth_req_tag()
        data_length = 0
        buff=b''
        buff = clientsocket.recv(8192) 
        
        logmodule.logmsg("buff_len="+str(len(buff))+" lastchksum="+str(lastchksum),'debug')
        
        
        #while(len(buff) ==0):
           #logmodule.logmsg("Broken Pipe",'debug')
           #buff = clientsocket.recv(8192) 
        if(len(buff) ==0):
            #clientsocket.close()
            return
        lastidx=len(buff)-1
        slastidx=len(buff)-2   
        chk_sum=(buff[lastidx]<<8)|buff[slastidx] 
        logmodule.logmsg("lastchksum="+str(lastchksum)+"chk_sum="+str(chk_sum),"debug")
        strbuff=""            
        for a in buff:
            strbuff=strbuff+","+str(a)
        #logmodule.logmsg(" rec_buff befor checksum check ="+strbuff+ " buff[3]="+str(buff[3]),'debug')
        while(chk_sum==lastchksum):
           logmodule.logmsg("Duplicate entry",'debug')
           strbuff=""            
           for a in buff:
              strbuff=strbuff+","+str(a)
           logmodule.logmsg("Duplicate entry ="+strbuff,'debug')
           buff = clientsocket.recv(8192)
           
           #while(len(buff) ==0):
               #logmodule.logmsg("inside checksum equal Broken Pipe",'debug')
               #buff = clientsocket.recv(8192) 
           if(len(buff) ==0):
              #clientsocket.close()
              return
           lastidx=len(buff)-1
           slastidx=len(buff)-2           
           chk_sum=(buff[lastidx]<<8)|buff[slastidx] 
           
       
        logmodule.logmsg("reqid="+str(buff[3]),'debug')
        buff_len=len(buff)-2
        calcchecksum=servfn.crc16(buff[0:buff_len])	
        
        if(chk_sum==calcchecksum):              
            
            logmodule.logmsg("rec_buff ="+strbuff+ " buff[3]="+str(buff[3]),'debug')
            
            if(buff[3]==1):
              
              res_sessionid=(buff[7]<<24)|(buff[6]<<16)|(buff[5]<<8)|buff[4] 
              #if(res_sessionid ==0):
              gsessionid=servfn.generate_sessionid()
              sessionid=gsessionid              
              otamode,resetmode,writemode,statemode,devid,sentmsg,challenge= datafn.requesthandler(buff,sessionid,ghashclient)                             
              logmodule.logmsg("inside auth devid="+devid+" sessionid="+str(sessionid) ,'debug')
              ncnt=0
              for x in ghashclient['client']:
               if x['devid']==devid:                 
                 x['writemode']=writemode
                 x['statemode']=statemode
                 x['resetmode']=resetmode
                 x['sessionid']=sessionid
                 x['otamode']=otamode
                 x['otaidx']=0
                 x['fwstartaddress']=0x20000
                 
                 ncnt=1                 
              if(ncnt==0):
               d={}
               d['devid']=devid              
               d['writemode']=writemode
               d['statemode']=statemode
               d['resetmode']=resetmode
               d['readmode']='false'
               d['sessionid']=sessionid
               d['otamode']=otamode
               d['otaidx']=0
               d['addr']=addr
               d['fwstartaddress']=0x20000
               d['checksum']=0
               ghashclient['client'].append(d)
                            
              clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
              clientsocket.sendall(sentmsg)
              strbuff=""
              for a in sentmsg:
                strbuff=strbuff+","+str(a)
              logmodule.logmsg("sent_msg auth="+strbuff,'debug')
              on_new_client(clientsocket,addr,0,sessionid) 
              
            if(buff[3]==2):
              sessionid=(buff[7]<<24)|(buff[6]<<16)|(buff[5]<<8)|buff[4] 
              lastchksum= chk_sum 
              readmode,sentmsg=datafn.logrequesthandler(buff,ghashclient,chk_sum)
              ncnt=0
              for x in ghashclient['client']:
               if x['sessionid']==sessionid:
                 x['readmode']=readmode
                 x['checksum']=chk_sum                
                 ncnt=1
              
              if(ncnt==0):
               d={}
               d['devid']=devid
               d['readmode']=readmode 
               d['writemode']='false'
               d['statemode']='false'
               d['resetmode']='false'  
               d['otamode']='false'
               d['otaidx']=0
               d['fwstartaddress']=0x20000
               d['sessionid']=sessionid 
               d['addr']=addr
                              
               ghashclient['client'].append(d)
               
                 
              clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
              clientsocket.sendall(sentmsg)
              strbuff=""
              for a in sentmsg:
                strbuff=strbuff+","+str(a)
              logmodule.logmsg("sent_msg log="+strbuff,'debug')
              print("recordcnt=",recordcnt)
              #if(recordcnt==configcnt):
              if(recordcnt>=400):#recursion limit exceeded error is thrown if record cnt > 450
                 logmodule.logmsg("client connection closed",'debug')
                 clientsocket.close()                 
              else:
                 on_new_client(clientsocket,addr,1,sessionid)  
            if(buff[3]==3):
             
              sessionid=(buff[7]<<24)|(buff[6]<<16)|(buff[5]<<8)|buff[4] 
              print("reqid=3 sessionid="+str(sessionid))
              
              rmode='false'
              readcnt=0
              for x in ghashclient['client']:
                if x['sessionid']==sessionid:
                   rmode=x['readmode']
                   wmode=x['writemode']
                   remode=x['resetmode']
                   smode=x['statemode']
                   omode=x['otamode']
                   devid=x['devid']
                   otaidx=x['otaidx']
                   fwstartaddress=x['fwstartaddress']
                   print("inside array")
              logmodule.logmsg("reqid=3, devid="+devid+", sessionid="+str(sessionid),'debug')     
              if(rmode=='true'):
                sentmsg= datafn.readhandler(sessionid,0) 
              elif(wmode=='true'):
                sentmsg,addloc= datafn.writehandler(devid,sessionid) 
              elif(remode=='true'):
                sentmsg,flagtype= datafn.resethandler(devid,sessionid)
              elif(smode=='true'):
                sentmsg,asserttype= datafn.statechangehandler(devid,sessionid)
              elif(omode=='true'):
                sentmsg,dataindex,fwstartaddress=datafn.otahandler(devid,sessionid,otaidx,fwstartaddress,'00.06.X.production.bin',buff)
                for x in ghashclient['client']:
                  if x['sessionid']==sessionid:
                   x['otaidx']=dataindex
                   x['fwstartaddress']=fwstartaddress
              if(rmode=='true' or wmode=='true' or remode=='true' or smode=='true'or omode=='true'):
                strbuff=""
                for a in sentmsg:
                   strbuff=strbuff+","+str(a)                
                logmodule.logmsg("sent_msg read/write/state/ota="+strbuff,'debug')
                time.sleep(1)         
                clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                clientsocket.sendall(sentmsg) 
                #time.sleep(1)     
                               
                on_new_client(clientsocket,addr,0,sessionid)              
              #clientsocket.close()
            if(buff[3]!=1 and buff[3]!=2 and buff[3]!=3):  
             sessionid=(buff[7]<<24)|(buff[6]<<16)|(buff[5]<<8)|buff[4] 
             for x in ghashclient['client']:
                if x['sessionid']==sessionid:
                   devid=x['devid']
                   rmode=x['readmode']
                   smode=x['statemode']
                   wmode=x['writemode']
                   remode=x['resetmode'] 
                   omode=x['otamode']
                   otaidx=x['otaidx']
                   
                   grmode=x['readmode']
                   gsmode=x['statemode']
                   gwmode=x['writemode']
                   gremode=x['resetmode'] 
                   gomode=x['otamode']
                   
             logmodule.logmsg("reqid="+str(buff[1])+", devid="+devid+", sessionid="+str(sessionid),'debug') 
             if(buff[1]==12 and omode=='true' and otaidx!=-1 ):
                logmodule.logmsg(" OTA write for deviceid="+devid + " otaidx="+str(otaidx) ,'debug')
                for x in ghashclient['client']:
                 if x['sessionid']==sessionid:
                   rmode=x['readmode']
                   wmode=x['writemode']
                   remode=x['resetmode']
                   smode=x['statemode']
                   omode=x['otamode']
                   devid=x['devid']
                   otaidx=x['otaidx']
                   fwstartaddress=x['fwstartaddress']
                sentmsg,dataindex,fwstartaddress=datafn.otahandler(devid,sessionid,otaidx,fwstartaddress,'00.06.X.production.bin',buff)
                strbuff=""
                for a in sentmsg:
                   strbuff=strbuff+","+str(a) 
                logmodule.logmsg("sent_msg ota otaidx="+str(otaidx)+" packet  ="+strbuff,'debug')  
                time.sleep(2)         
                clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                clientsocket.sendall(sentmsg) 
                for x in ghashclient['client']:
                  if x['sessionid']==sessionid:
                   x['otaidx']=dataindex
                   x['fwstartaddress']=fwstartaddress
                on_new_client(clientsocket,addr,0,sessionid) 
             if(buff[1]==12 and omode=='true' and otaidx==-1 ):
               logmodule.logmsg(" OTA completed for deviceid="+devid + " going to update db" ,'debug')
               
               datafn.UpdateOTAStatus(devid)
               sentmsg=datafn.configExitOTA(devid,sessionid)
               strbuff=""
               for a in sentmsg:
                   strbuff=strbuff+","+str(a) 
               logmodule.logmsg("sent_msg configExitOTA packet  ="+strbuff,'debug')  
               time.sleep(2)         
               clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
               clientsocket.sendall(sentmsg) 
               
               for x in ghashclient['client']:
                      if x['sessionid']==sessionid:
                         #x['otamode']='false' 
                         x['otaidx']=0 
               '''
               logmodule.logmsg("exiting from write in write sent_msg","debug")             
               sentmsg=datafn.configExit(devid,sessionid)
               time.sleep(2)
               strbuff=""
               for a in sentmsg:
                    strbuff=strbuff+","+str(a)                
               logmodule.logmsg("sent_msg ota configExit="+strbuff,'debug')
               clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
               clientsocket.sendall(sentmsg)
               '''
               on_new_client(clientsocket,addr,0,sessionid)                
             if(buff[1]==5 and wmode=='true' ):
              logmodule.logmsg(" reqid=5 going to write",'debug')
              datafn.writeUpdatehandler(devid,addloc)
              #added for no of days
              if(addloc=='FE18' or addloc=='0xFE18'):
                 sentmsg=datafn.commonflaghandler(devid,sessionid,'noofdays')
                 strbuff=""
                 for a in sentmsg:
                    strbuff=strbuff+","+str(a)                
                 logmodule.logmsg("sent_msg write addloc=='FE18' common flag="+strbuff,'debug')
                 
                 time.sleep(2)
                 clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                 clientsocket.sendall(sentmsg)    
              # end nof of daya             
              for x in ghashclient['client']:
                      if x['devid']==devid:
                         x['writemode']='false'              
              logmodule.logmsg("exiting from write in write sent_msg","debug")             
              sentmsg=datafn.configExit(devid,sessionid)
              time.sleep(2)
              strbuff=""
              for a in sentmsg:
                    strbuff=strbuff+","+str(a)                
              logmodule.logmsg("sent_msg write configExit="+strbuff,'debug')
              clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
              clientsocket.sendall(sentmsg)
             if(buff[1]==5 and remode=='true' ):
              logmodule.logmsg(" reqid=5 going to remode",'debug')
              datafn.resetUpdatehandler(devid,flagtype) 
              for x in ghashclient['client']:
                      if x['devid']==devid:
                         x['resetmode']='false'              
              logmodule.logmsg("exiting from remode in remode sent_msg","debug")             
              sentmsg=datafn.configExit(devid,sessionid)
              time.sleep(2)
              strbuff=""
              for a in sentmsg:
                strbuff=strbuff+","+str(a)
                
              logmodule.logmsg("sent_msg config exit resetmode="+strbuff,'debug')
              clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
              clientsocket.sendall(sentmsg)
             if(buff[1]==5 and omode=='true' ):
                for x in ghashclient['client']:
                      if x['sessionid']==sessionid:
                         x['otamode']='false' 
                         x['otaidx']=0 
               
                logmodule.logmsg("exiting from write in write sent_msg","debug")             
                sentmsg=datafn.configExit(devid,sessionid)
                time.sleep(2)
                strbuff=""
                for a in sentmsg:
                    strbuff=strbuff+","+str(a)                
                logmodule.logmsg("sent_msg ota configExit="+strbuff,'debug')
                clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                clientsocket.sendall(sentmsg)             
             if(buff[1]==5 and smode=='true' ):
              logmodule.logmsg(" reqid=5 going to state change",'debug')
              datafn.statechangeUpdatehandler(devid,asserttype) 
              sentmsg=datafn.commonflaghandler(devid,sessionid,asserttype)
              strbuff=""
              for a in sentmsg:
                strbuff=strbuff+","+str(a)
                
              logmodule.logmsg("sent_msg state common flag="+strbuff,'debug')
              time.sleep(5)
              clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
              clientsocket.sendall(sentmsg) 
              logmodule.logmsg("exiting from state in state sent_msg","debug") 
              for x in ghashclient['client']:
                      if x['devid']==devid:
                         x['statemode']='false'              
              logmodule.logmsg("exiting from state in state sent_msg","debug")             
              sentmsg=datafn.configExit(devid,sessionid)
              strbuff=""
              for a in sentmsg:
                strbuff=strbuff+","+str(a)
                
              logmodule.logmsg("sent_msg state configExit="+strbuff,'debug')
              time.sleep(5)
              clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
              clientsocket.sendall(sentmsg)
              
             if(buff[1]==6):
              try:
                strbuff=""            
                for a in buff:
                   strbuff=strbuff+","+str(a)
                #logmodule.logmsg("inside read readcnt="+str(readcnt)+" rec_buff="+strbuff,'debug')              
                rcnt=0
                for x in greadarray['client']:
                   if x['devid']==devid:
                      readcnt=x['readcnt']
                      readbuff=x['readbuff'] 
                      rcnt=1
                if(rcnt==0):
                    d={}
                    d['devid']=devid
                    d['readcnt']=readcnt
                    d['readbuff']=readbuff                           
                    greadarray['client'].append(d)   
                                       
                logmodule.logmsg("read start devid="+devid+" readcnt="+str(readcnt),'debug')
                readcnt+=1                
                totlen=len(buff)
                lastdataidx=totlen-2
                bufr=buff[11:lastdataidx]
                logmodule.logmsg("reqid=6 ,buff len="+str(len(buff))+",bufr len="+str(len(bufr)) ,'debug') 
                if(readcnt==5 or readcnt==6):
                   readbuff=bytearray()
                
                for i  in bufr:
                  readbuff.append(i)
                  
                if(readcnt==4):
                  datafn.readdatahandler(readbuff,devid)
                  logmodule.logmsg("read devid="+devid+" readcnt="+str(readcnt),'debug')
                  readbuff=bytearray()
                  for x in greadarray['client']:
                   if x['devid']==devid:
                      x['readcnt']=readcnt
                      x['readbuff']=readbuff 
                  sentmsg= datafn.readhandler(sessionid,readcnt)
                  time.sleep(1)
                  strbuff=""
                  for a in sentmsg:
                     strbuff=strbuff+","+str(a)                
                  logmodule.logmsg("sent_msg read ="+strbuff,'debug')
                  
                  clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                  clientsocket.sendall(sentmsg)  
                  on_new_client(clientsocket,addr,0,sessionid) 
                elif(readcnt==5):
                  datafn.readdatasoc(devid,readbuff)
                  logmodule.logmsg("read devid="+devid+" readcnt="+str(readcnt),'debug')
                  readbuff=bytearray()
                  for x in greadarray['client']:
                   if x['devid']==devid:
                      x['readcnt']=readcnt
                      x['readbuff']=readbuff 
                  sentmsg= datafn.readhandler(sessionid,readcnt)
                  time.sleep(1)
                  strbuff=""
                  for a in sentmsg:
                     strbuff=strbuff+","+str(a)                
                  logmodule.logmsg("sent_msg read ="+strbuff,'debug')
                  
                  clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                  clientsocket.sendall(sentmsg)  
                  on_new_client(clientsocket,addr,0,sessionid)
                elif(readcnt==6):
                  datafn.readdatavftexit(devid,readbuff)
                  logmodule.logmsg("read devid="+devid+" readcnt="+str(readcnt),'debug')
                  readbuff=bytearray()
                  readcnt=0
                  for x in greadarray['client']:
                   if x['devid']==devid:
                      x['readcnt']=readcnt
                      x['readbuff']=readbuff 
                  for x in ghashclient['client']:
                      if x['devid']==devid:
                         x['readmode']='false'
                  logmodule.logmsg("exiting from read in read sent_msg","debug")                    
                  sentmsg=datafn.configExit(devid,sessionid)
                  time.sleep(1)
                  clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                  clientsocket.sendall(sentmsg) 
                else:                  
                  for x in greadarray['client']:
                   if x['devid']==devid:
                      x['readcnt']=readcnt
                      x['readbuff']=readbuff 
                  logmodule.logmsg("read inside else devid="+devid+" readcnt="+str(readcnt),'debug')
                  sentmsg= datafn.readhandler(sessionid,readcnt)
                  time.sleep(1)
                  strbuff=""
                  for a in sentmsg:
                     strbuff=strbuff+","+str(a)                
                  logmodule.logmsg("sent_msg read ="+strbuff,'debug')
                  
                  clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                  clientsocket.sendall(sentmsg)  
                  on_new_client(clientsocket,addr,0,sessionid)  
                
              except:
                  logmodule.logmsg("exception in read sent_msg","error")             
                  sentmsg=datafn.configExit(devid,sessionid)
                  clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)   
                  clientsocket.sendall(sentmsg)  
                  #on_new_client(clientsocket,addr,0,sessionid) 
              
                              
        else:
            #clientsocket.close()
            print("checksum not equal")
            return
            
    except OSError as exc:
            print("exception ")
            #clientsocket.close()
            raise
            if exc.errno == errno.EBADF:
                pass # Socket is closed, so can't send SecureClose request.
            else:
                raise
    except socket.error as e:
        if e.errno == errno.ECONNRESET:
            logmodule.logmsg("errno.ECONNRESET",'error')
        else:
          if(grmode=='true' or gwmode=='true' or gremode=='true' or gsmode=='true'or gomode=='true'):
            sentmsg=datafn.configExit(devid,sessionid)
          raise
gsessionid=0
ghashclient={"client":[]}
greadarray={"client":[]}
grmode='false'
gwmode='false' 
gremode='false'
gsmode='false'
gomode='false'
def main():
  global recordcnt
  global gsessionid
  global lastchksum
  serverport=int(config.get('server-config', 'serverport'))
  
  serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  serv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  serv.bind(('0.0.0.0',serverport)) #please do not remove the bracket
  serv.listen(100)
  
  logmodule.logmsg("inside main",'debug')
  servfn=ServerFunctions()
  #executor = ThreadPoolExecutor(max_workers=100)
 
  try:    
    while True: 
        recordcnt=0
        conn, addr = serv.accept()
        logmodule.logmsg("Got connection from "+ str(addr)+ ' gsessionid='+str(gsessionid) ,'debug')
       
        t = Thread(target = on_new_client, args =(conn,addr,0,0 ), daemon = True) 
        t.start() 
        #t.join()
        
        #p1 = multiprocessing.Process(target=on_new_client, args =(conn,addr,0,0 ))
        #p1.start()
        #p1.join()
        #
        #task=executor.submit(on_new_client,(conn,addr,0,0 ))
  except Exception as exc:
       logmodule.logmsg("generated an exception: {exc}  "+str(exc),'error')
       
  finally:
      logmodule.logmsg("Closing socket",'debug')
      #serv.close()
  
if __name__ == "__main__":
    main()

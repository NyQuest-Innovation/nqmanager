import logging 
from logging.handlers import TimedRotatingFileHandler
import os
import time
import datetime
class logModule:
 
 #logger=this.myLogger() 
 #def myLogger(self):
 
 logHandler = TimedRotatingFileHandler("serverlogfile.csv",when="midnight",interval=1,backupCount=5)
 logFormatter = logging.Formatter('%(asctime)s,%(message)s') #logging.Formatter('%(asctime)s,%(devid)s, %(message)s')
 logHandler.setFormatter( logFormatter )
 logger = logging.getLogger( 'MyLogger' )
 logger.addHandler( logHandler )
 logger.setLevel( logging.DEBUG )
 #   return logger 
        
 def logmsg(self,devid,msg,mode):
  logname = "csvlogfile.csv"
  #print(msg)
  
  if(mode=='debug'):
    self.logger.debug(devid+','+msg) 
  if(mode=='info'):
    self.logger.info(devid+','+msg)
  if(mode=='warning'):
    self.logger.warning(devid+','+msg)
  if(mode=='error'):
    self.logger.error(devid+','+msg) 
  if(mode=='critical'):
    self.logger.critical(devid+','+msg) 
    
    
    

  

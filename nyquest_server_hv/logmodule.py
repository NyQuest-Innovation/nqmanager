import logging 
from logging.handlers import TimedRotatingFileHandler
import os
import time
import datetime
class logModule:
 
 #logger=this.myLogger() 
 #def myLogger(self):
 
 logHandler = TimedRotatingFileHandler("logfile.log",when="midnight",interval=1,backupCount=5)
 logFormatter = logging.Formatter('%(asctime)s %(message)s')
 logHandler.setFormatter( logFormatter )
 logger = logging.getLogger( 'MyLogger' )
 logger.addHandler( logHandler )
 logger.setLevel( logging.DEBUG )
 #   return logger 
        
 def logmsg(self,msg,mode):
  logname = "logfile.log"
  #print(msg)
  
  if(mode=='debug'):
    self.logger.debug(msg) 
  if(mode=='info'):
    self.logger.info(msg)
  if(mode=='warning'):
    self.logger.warning(msg)
  if(mode=='error'):
    self.logger.error(msg) 
  if(mode=='critical'):
    self.logger.critical(msg) 
    
    
    

  
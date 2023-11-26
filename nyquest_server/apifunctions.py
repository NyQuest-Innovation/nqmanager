import requests
import json
from logmodulecsv import logModule
class ApiFunctions:

  def apigetcalls(self,fnName):
     logmod= logModule()
     try:
      #http://132.148.88.157:88/nyquestweb/API/api/index.php/updatestateduration_hour/A12XA1000005
      apiurl="https://web.energy24by7.com/API/api/index.php/"+fnName
      r=requests.get(url = apiurl, params = '')
      return r

     except:
      
      logmod.logmsg("Exception apicall "+fnName,str(sys.exc_info()[0]),'error')
      return "error"
      
  def apipostcalls(self,fnName,params):
     logmod= logModule()
     try:
      #http://132.148.88.157:88/nyquestweb/API/api/index.php/updatestateduration_hour/A12XA1000005
      apiurl="https://web.energy24by7.com/API/api/index.php/"+fnName
      r=requests.post(url = apiurl, data =json.dumps(params))
      return r

     except:
      
      logmod.logmsg("Exception apipost "+fnName,str(sys.exc_info()[0]),'error')
      return "error"
  

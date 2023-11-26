import requests
from logmodule import logModule
class ApiFunctions:
  logmod= logModule()
  def apigetcalls(self,fnName):
    logmod= logModule()
    logmod.logmsg("inside apicall fname="+fnName,'debug')
    try:
      #http://132.148.88.157:88/nyquestweb/API/api/index.php/updatestateduration_hour/A12XA1000005
      apiurl="https://web.nyquestindia.com/API/api/index.php/"+fnName
      r=requests.get(url = apiurl, params = '')
      return r

    except:
        logmod.logmsg("Exception apicall=",'error')
        return "error"
  

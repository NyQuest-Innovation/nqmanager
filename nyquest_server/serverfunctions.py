

import socket,struct,sys
from ctypes import *
from serverclass import CommonMessageHeader

class ServerFunctions:
 


  def SWAPNIBBLE(self,x): 
    return (((x) << 4) | ((x) >> 4)) 


  def crc16(self,buff_p):
    '''
    CRC-16-CCITT Algorithm
    '''
    poly=0x8408
    #data = bytearray(data)
    #lendt=len(buff_p)
    #print("lendt=",lendt)
    crc = 0xFFFF
    idx=0
    #while True:
    for b in buff_p:
        cur_byte = 0xFF & b
        for _ in range(0, 8):
            if (crc & 0x0001) ^ (cur_byte & 0x0001):
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
            cur_byte >>= 1
          
      #lendt-=1
      #if(lendt==0):
        #break
    
    crc = (~crc & 0xFFFF)
   
    crc = (crc << 8) | ((crc >> 8) & 0xFF)
    return crc & 0xFFFF
    
 #23754
 

  def verify_checksum(self,request):        
     chksum_rcvd = request.chk_sum

     ''' Compute check sum of packet '''
     chksum_comp = crc_16(request);

     ''' Compare check sum '''
     if(chksum_rcvd == chksum_comp):
        return SUCCESS
     else:
        return "checksum missmatch"


 
  def ncrypt(self,buff_p, blen):
   swap_key1 = 0x55;
   swap_key2 = 0xAA;
  
   if((blen != 0) and (blen & 0x01)):
     blen-=1
   
   for buff_idx_count in range(0,blen-1,2):  
     #print("1=",buff_p[buff_idx_count])
     buff_encr_val1 = (self.SWAPNIBBLE(buff_p[buff_idx_count]) ^ swap_key1)
     buff_encr_val2 = (self.SWAPNIBBLE(buff_p[buff_idx_count + 1]) ^ swap_key2)
     
     if(buff_encr_val1>256):
        buff_encr_val1=buff_encr_val1 & 0x0FF

     
     if(buff_encr_val2>256):
        buff_encr_val2=buff_encr_val2 & 0x0FF
     
     
     buff_p[buff_idx_count] = buff_encr_val2
     buff_p[buff_idx_count + 1] =buff_encr_val1 
  
   return buff_p

  def dcrypt(self,buff_p, blen):
   
   swap_key1 = 0x55;
   swap_key2 = 0xAA;
   #if((blen != 0) and (blen & 0x01)):
     #blen-=1
   #print("blen=",blen)
   for buff_idx_count in range(0,blen-1,2):
     #print("1=",buff_p[buff_idx_count])
     #print("1=",buff_idx_count)
     buff_decr_val1 = (self.SWAPNIBBLE(buff_p[buff_idx_count] ^ swap_key2))
     buff_decr_val2 = (self.SWAPNIBBLE(buff_p[buff_idx_count + 1] ^ swap_key1))
     if(buff_decr_val1>256):
        buff_decr_val1=buff_decr_val1 & 0x0FF

     
     if(buff_decr_val2>256):
        buff_decr_val2=buff_decr_val2 & 0x0FF

     buff_p[buff_idx_count] = buff_decr_val2
     buff_p[buff_idx_count + 1] = buff_decr_val1
     
   return buff_p
	
    
  def get_instance_from_buffer(self,buffer):
    header_len = sizeof(CommonMessageHeader)
    if header_len > len(buffer):
        raise ValueError(-1, "Buffer smaller than header")
    header = CommonMessageHeader.from_buffer_copy(buffer)
    required_buf_len = header_len + header.data_len
    
    if required_buf_len > len(buffer):
        raise ValueError(-2, "Buffer smaller than header and data")
    return header.response,required_buf_len



  def print_ctypes_instance_info(self,obj, indent="", metadata="", max_array_items=10, indent_increment="    "):
    if metadata:
      print("{:s}{:s}: {:}".format(indent, metadata, obj))
    else:
      print("{:s}[{:}]: {:}".format(indent, type(obj), obj))
    if isinstance(obj, Structure):

      for field_name, field_type in obj._fields_:
        print_ctypes_instance_info(getattr(obj, field_name),indent=indent + indent_increment, metadata="[{:}] {:s}".format(field_type, field_name))
        if isinstance(obj, Array):
          if max_array_items < len(obj):
            items_len, extra_line = max_array_items, True
          else:
            items_len, extra_line = len(obj), False
          for idx in range(items_len):
            print_ctypes_instance_info(obj[idx], indent=indent + indent_increment, metadata="[{:d}]".format(idx))
          if extra_line:
            print("{:s}...".format(indent + indent_increment))
            
  def generate_incrementer(self,start):
       num = start
       num += 1
       return num
  def generate_sessionid(self):
      import random
      n = random.randint(2,100000)
      return n
  def Bit(self,x):
      return 1<<x
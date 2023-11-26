from ctypes import *


class CommonMessageHeader(Structure):
    _pack_ = 1
    _fields_ = [
        ("sof", c_uint),
        ("request_id", c_uint),
        ("interface", c_uint),
        ("msg_type", c_uint),
        ("response", c_uint),
        ("data_len", c_int),
    ]

class auth_req_payload(Structure):     
   _fields_ =[("device_id",c_uint8*12),
	       ("dev_type", c_uint8),
	       ("hw_version",c_uint16),
	       ("fw_version", c_uint16),
	       ("challenge",c_uint32)]

class auth_req_tag(Structure):     
   _fields_ =[("syncword",c_uint8*3),
               ("req_id",c_uint8),
	       ("sessionid", c_uint32),
	       ("seqid",c_uint8),
               ("proto_major_ver",c_uint8),
               ("proto_minor_ver",c_uint8),
               ("payload_len",c_uint32),
	       ("payload",c_char*2560),
	       ("chk_sum",c_uint16)]
   

class auth_res_payload(Structure):
	_fields_ =[("seq_id",c_uint8),
		   ("time_stamp", c_uint8*6),
	           ("sesn_id",c_uint32),
	           ("challenge_res", c_uint32),
	           ("res_flags",c_uint16)]


class auth_res_tag(Structure):     
   _fields_ =[("syncword",c_uint8*3),
               ("req_id",c_uint8),
	       ("payload_len",c_uint32),
	       ("payload",c_char*2560),
	       ("chk_sum",c_uint16)]

class log_rec_tag(Structure):
	_fields_ =[("log_time",c_uint8*5),
		   ("bat_v",c_float),
		   ("bat_chg_i",c_float),
		   ("bat_dis_i",c_float),
		   ("sol_v",c_float),
		   ("sol_i",c_float),
		   ("ac_load_i",c_float),
		   ("temp_adc", c_uint16),
		   ("bat_soc",c_float),
		   ("switch_stat",c_uint8),
		   ("sum_sol_e",c_float),
		   ("sum_sol_bat_e",c_float),
		   ("sum_bat_load_e",c_float),
		   ("sum_ac_chg_e",c_float),
		   ("sum_ac_load_e",c_float),
		   ("num_samples",c_uint16),
		   ("log_type",c_uint8),
           ("state_change_flag",c_uint8),
           ("dev_status_flag",c_uint16),
           ("algorithm_status_flag",c_uint16),
           ("inverter_volt",c_float),
           ("error",c_uint8)]


class log_res_tag(Structure):
	_fields_ =[("seq_id",c_uint8),
		   ("res_flags",c_uint16)]
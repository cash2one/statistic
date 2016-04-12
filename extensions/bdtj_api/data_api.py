#coding=utf-8
import urllib2
import urllib
import gzip
import StringIO
import json
import httplib
import sys
import base64
import binascii
import zlib
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5


key = "-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDdNjuYkrcNa3+QWjqL++ColD"\
    "XcpzyZvvAQHsUpp3jQMWoe3GVdFxm6n8HlTcLmRS8u8Pb/fq8Ii3xofwbCyVAU6yZ+U3jb+Yq"\
    "/Qul0iW1bScMjrQG41IOkaYxm1GTiT5sm83737TnoBl267cEJEnGCTeqfzGx15Zun66bjaPi4uwIDAQAB\n"\
    "-----END PUBLIC KEY-----"
token = u'908ed91bfdfae86c7e42ab50b5953a01'
user_name = u'kgb-mobile'
password = u'kgb@mobile'
log_url = u'https://api.baidu.com/sem/common/HolmesLoginService'
api_url = u'https://api.baidu.com/json/tongji/v1/ProductService/api'
uuid = u'MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBg-1'

def compress_string(strs):
    file_obj = StringIO.StringIO();
    with gzip.GzipFile(fileobj=file_obj, mode="w") as f:
        f.write(strs)
    comp_str = file_obj.getvalue()
    return comp_str

def decompress_string(strs):
    file_obj = StringIO.StringIO(strs)
    gzip_tool = gzip.GzipFile(fileobj=file_obj,mode='rb')
    return gzip_tool.read()

def pre_login():
    pre_login_request = {u'osVersion':u'Windows',u'deviceType':u'pc',u'clientVersion':u'1.0'}
    text = get_response(u'preLogin',pre_login_request)



def do_login():
    do_login_request = {u'password':password}
    get_response(u'doLogin',do_login_request)

def do_logout(ucid,st):
    do_logout_request = {u'ucid':ucid,u'st':st}
    get_response(u'doLogout',do_logout_request)

def get_response(functionName,request):
    params = {u'username':user_name,u'token':token,u'functionName':functionName,u'uuid':uuid,u'request':request}
    params_json = json.dumps(params)
    commpress_data = compress_string(params_json)
    commpress_data = zlib.compress(params_json,9)
    index = 0
    encrypt_data=''
    rsa_obj = RSA.importKey(key)
    cipher = PKCS1_v1_5.new(rsa_obj)
    while(index<len(commpress_data)):
        pack_data = commpress_data[index:index+117]
        encrypt_data += cipher.encrypt(pack_data)
        index += 117
    header ={u'uuid':uuid,u'account_type':u'1'}
    request = urllib2.Request(log_url,binascii.hexlify(encrypt_data),header)
    response = urllib2.urlopen(request)
    text = response.read()
    return_code = binascii.hexlify(text[0:2]).decode(u'utf-8')
    print return_code
    return text


def query(ucid,st,date):
    start_time = date + u'000000'
    end_time = date + u'235959'
    header = {u'UUID':uuid,u'USERID':ucid}
    auth_header = {u'username':user_name,u'password':password,u'token':token,u'account_type':1}
    query_parameter_type = {u'reportid':1,u'siteid':8186823,u'metrics':u'stayTime',u'start_index':0,u'max_result':10,
        u'start_time':start_time,u'end_time':end_time}
    api_request = {u'serviceName':u'report',u'methodName':u'query',u'parameterJSON':query_parameter_type}
    holmes_request = {u'header':auth_header,u'body':api_request}
    params_json = json.dumps(holmes_request)
    request = urllib2.Request(api_url,params_json,header)
    response = urllib2.urlopen(request)
    text = response.read()

if __name__ == "__main__":
    pre_login()
    #do_login()
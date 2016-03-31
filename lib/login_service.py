# coding=utf-8
import os
import json
import zlib
import rsa_public_encrypt as rsa

class LoginService(object):
    def __init__(self):
        pass

    def init(self, url, uuid, account_type, pubkey):
        self.url = url
        self.pubkey = "%s/%s" % (os.path.dirname(__file__), pubkey)
        self.headers = []
        self.headers.append('UUID: %s' % uuid)
        self.headers.append('account_type: %s' % account_type)
        self.headers.append('Content-Type:  data/gzencode and rsa public encrypt;charset=UTF-8')
        
    def gen_post_data(self, data):
        index = 0
        json_data = json.dumps(data)
        gz_data = zlib.compress(json_data, 9)
        rsa_obj = rsa.RsaPublicEncrypt(self.pubkey)
        en_data = ""
        while(index < len(gz_data)): 
            gz_pack_data = gz_data[index:index + 117]
            index += 117
            en_data += rsa_obj.pub_encrypt(gz_pack_data)
        
        print en_data

    def post(self, data):
        print data
        send_data = self.gen_post_data(data) 

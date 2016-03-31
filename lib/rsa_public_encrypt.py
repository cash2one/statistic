# coding=utf-8

import rsa

class RsaPublicEncrypt(object):
    def __init__(self, path):
        self.path = path
        self.public_key = self.get_public_key(path)

    def get_public_key(self, path):
        return open(path).read()

    def pub_encrypt(self, data):
        if not isinstance(data, basestring):
            encrypted = None
        else:
            pubkey = rsa.PublicKey.load_pkcs1(self.public_key) 
            encrypted = rsa.encrypt(data, pubkey)
        return encrypted

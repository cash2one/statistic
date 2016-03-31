# coding=utf-8

import login_service as login
import bdtj_config as config

class BaiduTongjiApi(object):
    def __init__(self, username, token):
        self.username = username
        self.token = token

    def pre_login(self):
        print "----------------------preLogin----------------------\r"
        print "[notice] start preLogin!\r"
        login_obj = login.LoginService()
        login_obj.init(config.LOGIN_URL, config.UUID, config.ACCOUNT_TYPE, config.PUB_KEY)       
        pre_login_data = {}
        pre_login_data['username'] = self.username
        pre_login_data['token'] = self.token
        pre_login_data['functionName'] = 'preLogin'
        pre_login_data['uuid'] = config.UUID
        pre_login_data['request'] = {'osVersion': 'linux', 'deviceType': 'pc', 'clientVersion': '1.0'}

        login_obj.post(pre_login_data)

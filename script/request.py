#-*- coding: utf-8 -*-
import sys
import os
import httplib, urllib
import json

class Request(object):
    def post(self, host, port, url, data):
        post_data = json.dumps(data, ensure_ascii=False).encode("utf8")
        return self._request(host, port, url, post_data, "POST")      
 
    def get(self, host, port, url, data):
        if data: 
            get_data = urllib.urlencode(data)
            url = "%s?%s" % (url, get_data)
        return self._request(host, port, url, None, "GET")

    def _request(self, host, port, url, data, type):
        response = {}
        httpClient = None
        try:
            headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
            httpClient = httplib.HTTPConnection(host, port, timeout=60)
            if type == "GET":
                httpClient.request(type, url)
            else:
                httpClient.request(type, url, data, headers)
            resp = httpClient.getresponse()
            result = unicode(resp.read(), errors="ignore")
            if result is not None and len(result) > 0:
                response = json.loads(result)
        except Exception, e:
            print str(e)
        finally:
            if httpClient:
                httpClient.close()
        return response 

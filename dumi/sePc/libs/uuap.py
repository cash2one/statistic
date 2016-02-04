# coding=utf-8

__all__ = [
    "Client",           
]

import sys, os, re, json, base64
import requests

class Client:
    u"""能透明处理UUAP登录的客户端，使用此对象发出的所有Web请求，都在一个Session中。"""
    def __init__(self, username=None, password=None):
        self._dirty = False
        self._username = username or DEFAULT_USER
        self._password = password or DEFAULT_PASSWD
        self._s = requests.Session()
        
    def get(self, url):
        u"使用get方法获取网页，返回对象包含状态码、头部词典和HTML内容，其中头部词典的key统一使用小写"
        r = self._get(url)
        return self._build_page_result(r)

    def post(self, url, data=None):
        u"使用post方法获取网页，返回JSON结构，参见get方法"
        r = self._post(url, data)
        return self._build_page_result(r)

    def wget(self, url, data=None):
        u"返回页面二进制内容"
        if data == None:
            r = self._get(url)
        else:
            r = self._post(url, data)
        return r.content

    def _build_page_result(self, r):
        ret = {
            "status_code" : r.status_code,
            "headers" : Client.headers2dict(r.headers),
            "html" : r.text
        }
        return ret

    def _get(self, url):
        r = self._s.get(url)
        url = self._ensure_login(r, url)
        if url != None: r = self._s.get(url)
        return r

    def _post(self, url, data=None):
        r = self._s.post(url, data=data)
        url = self._ensure_login(r, url)
        if url != None: r = self._s.post(url, data=data)
        return r

    def _ensure_login(self, r1, target=None):
        u"""
            登录步骤：
            1.传入访问第三方平台url得到的r1对象，如果需要登录，则r1应该已经由Location跳转到登录页面1
            2.从登录页面1解析lt和execution的值，配合用户信息，构造POST数据提交，返回登录页面2（临时页面）
            3.忽略临时页面中要加载的两个img，直接get当前页面，并禁止自动跳转，从结果头部中拿到Location返回
        """
        if self._dirty:
            return
        self._dirty = True      # 只尝试一次登录
        
        s = self._s
        # 登录页面1
        #r1 = s.get(url)
        if not r1.url.startswith("https://uuap.baidu.com") and not r1.url.startswith("http://uuap.baidu.com"):
            # 没有跳转，判定为不需要登录
            return

        if r1.url.startswith("http://uuap.baidu.com:80/remoteLogin"):
            r1 = s.get("http://uuap.baidu.com/login?validated=true&service=%s" % (target))

        login_url = r1.url
        
        # 解析内容
        #<input type="hidden" name="lt" value="LT-719815-J4DkY7dSr6ADWMdYAYuIWUpdjWcICE" />
        #<input type="hidden" name="execution" value="e1s1" />
        reg = re.compile(ur"""<input\s*type\s*=\s*"hidden"\s*name\s*=\s*"(\w+)"\s*value\s*=\s*"([-\w]+)\"""")
        mclist = reg.findall(r1.text)

        lt = None
        execution = None
        if len(mclist) >= 2:
            for mc in mclist:
                if mc[0] == "lt":
                    lt = mc[1]
                elif mc[0] == "execution":
                    execution = mc[1]
        if not (lt and execution): raise Exception("登录失败：登录页解析出错，页面可能已经改版")

        # 构造POST数据
        data = {
            "username" : self._username,
            "password" : self._password,
            "rememberMe" : "on",
            "lt" : lt,
            "execution" : execution,
            "_eventId" : "submit",
            "type" : 1,
        }

        try:
            # 登录页面2（临时页面）
            r2 = s.post(login_url, data=data)

            if u"用户名不存在" in r2.text:
                raise Exception(u"登录失败：用户名'%s'不存在" % self._username)

            if u"密码错误" in r2.text:
                raise Exception("登录失败：密码错误")

            # 根据Location获取登录成功后待访问的URL
            r3 = s.get(login_url, allow_redirects=False)
            target_url = r3.headers["Location"]

        except Exception, data:
            raise Exception(u"登录失败：发生未知错误")

        return target_url

    @staticmethod
    def headers2dict(headers):
        ret = {}
        for key in headers:
            ret[key] = headers[key]
        return ret

    
DEFAULT_USER = "yanglingling01"
DEFAULT_PASSWD = "LPyll900119"

def main(dest):
    c = Client()
    #obj = c.get("http://cn.bing.com")
    obj = c.get(dest)
    print json.dumps(obj, indent=4, ensure_ascii=False)

#def get_test_user():
#    path = ur"D:\Backup\word"
#    with open(path, "rb") as fp:
#        word = base64.b64decode(fp.read())
#    return "kongliang", word

def savelogin(path):
    r = requests.get("http://family.baidu.com/portal/")
    cookies = {}
    for cookie in r.cookies:
        cookies[cookie.name] = cookie.value
    obj = { "cookies" : cookies, "text" : r.text }
    with open(path, "wb") as fp:
        s = json.dumps(obj, indent=4, ensure_ascii=False)
        fp.write(s.encode("utf-8"))


if __name__ == "__main__":
    main(sys.argv[1])
    #savelogin("tt.txt")

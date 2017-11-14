# coding=utf-8
import urllib
import json
import time
import sys
message=''

def real_get_accesstoken():
    try:
        appId = 'wx5c897ac4cb655a58'
        appSecret = '31f075c97a0cb2fb553a84cc1e15ff4e'

        postUrl = ("https://api.weixin.qq.com/cgi-bin/token?grant_type="
                   "client_credential&appid=%s&secret=%s" % (appId, appSecret))
        urlResp = urllib.urlopen(postUrl)
        urlResp = json.loads(urlResp.read())

        token = urlResp['access_token']
        lefttime = urlResp['expires_in']

        return  token
    except Exception,e:
        print e
        return ''
        

def sendsms(token):
    try:
        postUrl="https://api.weixin.qq.com/cgi-bin/message/mass/send?access_token=%s"%(token)
        
        postData="""
        {
   "touser":[
    "orqwvuL4zq8v8RPFSliElVj-5uGU",
    "orqwvuEIQhC7ithn41tsp9qidbFo"
   ],
    "msgtype": "text",
    "text": { "content": "%s"}
}
        """%(message)
        
        urlResp = urllib.urlopen(url=postUrl, data=postData)
        print urlResp.read()
        print '------------------------------'
    except Exception,e:
        print e
        return ''
        
def sendsms_forone(openid,token,message):
    try:
        print openid
        postUrl="https://api.weixin.qq.com/cgi-bin/message/custom/send?access_token=%s"%(token)
        
        postData="""
        {
    "touser":"%s",
    "msgtype":"text",
    "text":
    {
         "content":"%s"
    }
}
        """%(openid,message)
        
        urlResp = urllib.urlopen(url=postUrl, data=postData)
        print urlResp.read()
        print '------------------------------'
    except Exception,e:
        print e
        return ''

def wechat_send(message):
    #message=sys.argv[1]
    result=real_get_accesstoken()
    print result
    print message
    #sendsms(result)

    #sendlist=["orqwvuAs7g3veVOHgHJa5RWPHQPQ","orqwvuInUBtuzvFOPB4qWmlPqsQw","orqwvuL4zq8v8RPFSliElVj-5uGU","orqwvuEIQhC7ithn41tsp9qidbFo","orqwvuMv9Vj7xZvEazULt0YEEOSU","orqwvuKYJXv8jiQwljCZLZCLM85s"]
    sendlist=["oLnIT0sEkSgSAn9qnI5VMyQsMphc"]
    for one in sendlist:
        sendsms_forone(one,result,message)
        #sendsms_forone(one,result,message)
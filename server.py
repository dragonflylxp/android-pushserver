#coding:utf-8
#File: main.py
#Auth: lixp(@500wan.com)
#Date: 2014-10-17 17:19:49
#Desc: 

import tornado.tcpserver 
import tornado.ioloop
import tornado.gen
import tornado.websocket
import tornadoredis
import time
import json
import sys,os


#reids-svr
HOST = '192.168.41.141'
PORT = 6379
DB   = 0
TIMEOUT = 5
CALLBACK_PERIOD = 5 

#pushkey到conn的正排拉链
g_p2c = {}  
#conn到pushkey的倒排拉链
g_c2p = {} 

#redis客户端连接
g_redis_sub = None
g_redis_blpop = None


"""定时清空超时连接
"""
def update_connections():
    print 'remove timeout connection!',len(g_p2c),int(time.time())
    now  = time.time()
    keys = g_p2c.keys() 
    for p in keys:
        c = g_p2c[p]
        if c['t'] < now-CALLBACK_PERIOD:
            #超时关闭连接
            del g_c2p[c['c']]
            g_p2c[p]['c'].close()
            del g_p2c[p]


"""订阅消息并监听
"""
@tornado.gen.engine
def sub_listen():
    #异步提交cmd，必须yield
    yield tornado.gen.Task(g_redis_sub.subscribe, 'android_msg_pubsub')
    g_redis_sub.listen(sub_callback) 


"""消息订阅回调
"""
def sub_callback(msg):
    if msg.kind != 'message':
        return

    p = msg.body
    if g_p2c.has_key(p):
        #当客户端消息可用时主动推送
        key = 'android_push_' + p.lower()
        g_redis_blpop.blpop((key,), TIMEOUT, blpop_callback)
    else:
        #连接不可用时，被动拉取
        pass


"""消息发送回调
"""
def blpop_callback(msg):
    msg = msg.values()[0].encode('utf8')
    if msg:
        try:
            dct   = json.loads(msg)
            token = dct.get('token', None)
            if token and g_p2c.has_key(token):
                #兼容被动拉取消息格式
                resp = {}
                data = {}
                expire = dct.get('etc',{}).get('expire',0)
                if expire >= time.time(): 
                    data['msg'] = dct.get('data',{})
                else:
                    data['msg'] = {}
                data['interval'] = 350
                resp['data'] = data
                resp['code'] = '1'

                #发送msg
                try:
                    tornado.ioloop.IOLoop.instance().add_callback(lambda: \
                        g_p2c[token]['c'].write_message(json.dumps(resp,ensure_ascii=False)))
                except Exception as e:
                    print 'Connection already closed!:%s' % e
            else:
                #token不合法或客户端未连接，直接丢弃msg
                print 'unreachable token:[ TOKEN:%s ]' % token
        except Exception as e:
            print 'push msg err![ MSG:%s ERR:%s]' % (msg,e)
    
"""客户端连接
"""
class Connection(object):
    def __init__(self, stream, address):
        self._stream  = stream
        self._address = address
        self._stream.set_close_callback(self.on_close)
        self.read_handler()
        print "A new client has connected!", address

    def read_handler(self):
        if self._stream.closed():
            return 
        self._stream.read_until('\n', self.biz_handler)

    def biz_handler(self, data):
        req = data[:-1]
        if len(req) > 0:
            #首次通信:pushkey
            g_p2c.update({req:{'c':self, 't':time.time()}})
            g_c2p.update({self:req})
            resp = 'ACK\n'
        else:
            #心跳包:''
            g_p2c[g_c2p[self]]['t'] = time.time()
            resp = 'PING\n' 

        self.write_handler(resp)
        self.read_handler()

    def write_handler(self, data):
        if self._stream.closed():
            return 
        self._stream.write(data)

    def on_close(self):
        print "A connection has broken!", self._address

class PushServer(tornado.tcpserver.TCPServer):
    def handle_stream(self, stream, address):
        Connection(stream, address)

class MainHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print "A new client has connected!"

    def on_close(self):
        print "A connection has broken!"

    def on_message(self, message):
        req  = message
        if len(req) > 0:
            #首次通信:pushkey
            print 'First tcp pack: [ PUSHKEY:%s ]' % req
            g_p2c.update({req:{'c':self, 't':time.time()}})
            g_c2p.update({self:req})
            try:
                self.write_message('ACK')
            except Exception as e:
                print 'Connection already closed!:%s' % e
        else:
            #心跳包:'', 不响应心跳包节约流量
            print 'Heart beat to keepalive: [ PING:%s ]' % req
            g_p2c[g_c2p[self]]['t'] = time.time()

if __name__ == '__main__':
    """
    #启动tcp-server
    server = PushServer()
    server.bind(8774)
    server.start() # 0:Forks multiple sub-processes
    """
    application = tornado.web.Application([(r'/websocket',MainHandler),])
    application.listen(8774)

    #redis-subscribe
    g_redis_sub = tornadoredis.Client(host=HOST, port=PORT, selected_db=DB,
                                      io_loop=tornado.ioloop.IOLoop.instance())
    g_redis_sub.connect()
    sub_listen()

    #redis-blpop
    g_redis_blpop = tornadoredis.Client(host=HOST, port=PORT, selected_db=DB,
                                        io_loop=tornado.ioloop.IOLoop.instance())
    g_redis_blpop.connect()

    #定时清理超时connection
    tornado.ioloop.PeriodicCallback(update_connections, CALLBACK_PERIOD*1000).start() 
    
    #启动ioloop
    tornado.ioloop.IOLoop.instance().start()

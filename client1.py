#coding:utf-8
#File: test.py
#Auth: lixp(@500wan.com)
#Date: 2014-10-17 18:03:10
#Desc: 

import sys,os
import socket,time
import select
import errno

CHUNKSIZE = 4096 
DELIMITER = '\n'
_ERRNO_WOULDBLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)
TIMEOUT   = 3600.0
TIMEGAP   = 3.0
PING      = '\n'
CONN      = 'pushkey1\n'

HOST = '127.0.0.1'
PORT = 8774

def connect(host, port):
    sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sockfd.connect((host, port))
    #连接之后再设置非阻塞
    sockfd.setblocking(0)
    return sockfd

sockfd  = connect(HOST, PORT)
inputs  = [sockfd, sys.stdin]
outputs = [sockfd, sys.stdout]

timeout  = TIMEOUT
lefttime = TIMEGAP 
stime = etime = 0.0
readbuf  = '' 
writebuf = CONN    
cmd      = None 
while True:
    readable,writeable,exceptional = select.select(inputs, outputs, [], timeout) 
    for r in readable:
        if r is sockfd:
            try:
                chunk = r.recv(CHUNKSIZE)
            except socket.error as e:
                if e.args[0] in _ERRNO_WOULDBLOCK:
                    pass
                else: #其他异常就断开连接,重连 
                    print '======Read catch a exception:%s======' % e
                    r.close()
                    inputs.remove(r)
                    outputs.remove(r)
                    sockfd = connect(HOST, PORT)
                    inputs.append(sockfd)
                    outputs.append(sockfd)
                    readbuf  = ''
                    writebuf = CONN
                    continue

            #对方断开连接
            if not chunk:
                print '======Server broken the pipe======'
                r.close()
                inputs.remove(r)
                outputs.remove(r)
                sockfd = connect(HOST, PORT)
                inputs.append(sockfd)
                outputs.append(sockfd)
                readbuf  = ''
                writebuf = CONN
                continue

            #判断数据是否OK,保证读到分隔符
            readbuf = type(readbuf)().join([readbuf, chunk])
            pos =  readbuf.find(DELIMITER)
            data = None
            if pos != -1:
               data = readbuf[:pos+len(DELIMITER)] 
               readbuf = readbuf[pos+len(DELIMITER)+1:]

            if data:
                print int(time.time()),data[:-1]
                if data == 'ACK\n':
                    writebuf = PING

        else:
            cmd = sys.stdin.readline()
            if cmd == 'quit\n':
                break

    if cmd == 'quit\n':
        break

    if cmd == 'recon\n':
        sockfd.close()
        inputs.remove(sockfd)
        outputs.remove(sockfd)
        sockfd = connect(HOST, PORT)
        inputs.append(sockfd)
        outputs.append(sockfd)
        continue
    
    for w in writeable:
        err = False
        if w is sockfd:
            etime = time.time()
            #更新时间
            lefttime = max(lefttime-(etime-stime), 0)
            if lefttime > 0:
                timeout = lefttime
                stime = time.time()
                continue

            lefttime = TIMEGAP 
            stime = time.time()
            nlen  = len(writebuf)
            npos  = 0
            while npos < nlen: #相当于阻塞写
                nwrite = 0
                try:
                    nwrite = w.send(writebuf[npos:]) 
                except socket.error as e:
                    if e.args[0] in _ERRNO_WOULDBLOCK:
                        nwrite = 0
                        pass
                    else:
                        print '======Write catch a exception:%s======' % e
                        w.close()
                        inputs.remove(w)
                        outputs.remove(w)
                        sockfd = connect(HOST, PORT)
                        outputs.append(sockfd)
                        inputs.append(sockfd)
                        err = True
                        break
                npos  = npos + nwrite
            if err:
                continue

        else:
            if cmd:
                sys.stdout.write(cmd)

sockfd.close()


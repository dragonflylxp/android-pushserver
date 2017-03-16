## android消息推送服务

* 服务基于[tornado](http://www.tornadoweb.org/en/stable/)异步框架
* 用户设备通过websocket与推送服务保持长连接(心跳检测、僵尸连接处理)
* 基于[tornadoredis](https://github.com/leporo/tornado-redis/),服务订阅redis消息触发推送给用户

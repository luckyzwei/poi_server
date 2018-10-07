# -*- coding: utf-8 -*+
import logging
import time
from rediscluster import StrictRedisCluster
import redis

import settings

JSON_CONTENT_TYPE = "application/json"

# 初始化日志
logger = logging.getLogger('cloudgo.common')



# datetime转化时间戳
def trans_time_format(datetime):
    mytime = time.strptime(datetime, '%Y-%m-%dT%H:%M:%S.%f')
    # tmytime = time.strftime("%Y-%m-%d %H:%M:%S", mytime)
    tmytime = time.mktime(mytime)
    return tmytime


def trans_time(datetime):
    mytime = time.strptime(datetime, '%Y-%m-%dT%H:%M:%S')
    tmytime = time.mktime(mytime)
    return tmytime


def trans_time_formation(datetime, format):
    mytime = time.strptime(datetime, format)
    tmytime = time.mktime(mytime)
    return tmytime



class RedisCache(object):
    def __init__(self):
        self.redis_nodes = settings.REDIS_NODES
        self.password =settings.REDIS_PASSWORD
        self.max_connections=settings.MAX_CONNECTIONS
        try:
            kwargs = {}
            if self.password:
                kwargs = {'password': self.password}
            self._connection = StrictRedisCluster(max_connections=self.max_connections,
                                                  startup_nodes=self.redis_nodes,
                                                  **kwargs)

        except Exception, e:
            logger.error('redis 链接错误请检查链接配置或查看redis是否可用')

    @staticmethod
    def create_pool(self):

        redis_config = self.redis_nodes[0]
        redis_config['password'] = self.password
        redis_config['max_connections'] = self.max_connections
        RedisCache.pool = redis.ConnectionPool(
                **redis_config
            )


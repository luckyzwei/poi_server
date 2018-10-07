# -*- coding: utf-8 -*-
DEV = True
HOST = '0.0.0.0'
PORT = 64690
LOG_PATH = '/data/hq/log/'
AMAP_TOPIC_NAME = 'Amap_POI_Test'
GOOGLE_TOPIC_NAME = 'Google_POI_Test'

AMAP_REQ_BASE_URL = 'http://restapi.amap.com/v3/place/text?'
AMAP_REQ_KEY = '64042a21731983a666f0e680db4c5d22'

GOOGLE_REQ_BASE_URL = 'http://maps.googleapis.com/maps/api/place/%s/json?'
GOOGLE_REQ_KEY = 'AIzaSyDL2Zs3rtKLDvkZgPL5Cz9tXDwcqbi0Yvo'
PROXIES = {'http': '198.11.181.175:9556'}


REDIS_POI_SEARCH_HEARD = 'poi_search:'

KAFKA_CLUSTER = 'myhost:9092'
KAFKA_ENABLE_AUTO_COMMIT = False
KAFKA_HEARTBEAT_INTERVAL_MS = 30000
KAFKA_SESSION_TIMEOUT_MS = 50000
KAFKA_KEY_DESERIALIZER = 'org.apache.kafka.common.serialization.StringDeserializer'
KAFKA_VALUE_DESERIALIZER = 'value.deserializervalue.deserializer'
if DEV:
    SEARCH_SERVER_IP = '118.126.117.140'
    SEARCH_SERVER_PORT = '9002'

    CODIS_SERVER_HOST = '118.126.117.140'
    CODIS_SERVER_PORT = '6379'
    CODIS_SERVER_PASSWORD = None
else:
    SEARCH_SERVER_IP = 'internal-search-server.haoqiao.com'
    SEARCH_SERVER_PORT = '9001'

    CODIS_SERVER_HOST = 'codis-test.haoqiao.com'
    CODIS_SERVER_PORT = '19001'
    CODIS_SERVER_PASSWORD = None

# 1：等待leader只将记录写入其本地日志
# 0：生产者不会等待来自服务器的任何确认[
# 'all'：等待完整的同步副本集写入记录
KAFKA_ACKS = 0
KAFKA_RETRIES = 0
KAFKA_BATCH_SIZE = 16384
KAFKA_KEY_SERIALIZER = 'org.apache.kafka.common.serialization.StringSerializer'
KAFKA_VALUE_SERIALIZER = 'org.apache.kafka.common.serialization.StringSerializer'

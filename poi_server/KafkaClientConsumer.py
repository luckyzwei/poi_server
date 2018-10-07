# coding: utf-8
import thread
import traceback

from kafka import KafkaConsumer, KafkaProducer
import settings
from public.logger import simple_log

logger = simple_log()


class KafkaClientConsumer(object):

    def __init_property(self, str_topic_name):
        """
        #消费者 初始化配置
        :param str_topic_name:
        :return:
        """
        _props = {}
        _props['bootstrap_servers'] = settings.KAFKA_CLUSTER
        _props['group_id'] = 'G_%s' % str_topic_name
        _props['enable_auto_commit'] = settings.KAFKA_ENABLE_AUTO_COMMIT
        _props['heartbeat_interval_ms'] = settings.KAFKA_HEARTBEAT_INTERVAL_MS
        _props['session_timeout_ms'] = settings.KAFKA_SESSION_TIMEOUT_MS
        # _props['key_deserializer'] = settings.KAFKA_KEY_DESERIALIZER
        # _props['value_deserializer'] = settings.KAFKA_VALUE_DESERIALIZER
        print _props
        return _props

    def __init__(self, str_topic_name):
        _props = self.__init_property(str_topic_name)
        self._topic_name = str_topic_name
        self._consumer_obj = KafkaConsumer(str_topic_name,**_props)
        logger.info('duan add,threadId:%s' % thread.get_ident())

    @classmethod
    def get_consumer_instance(cls):
        """
        #获取实例
        :param str_topic_name:
        :return:
        """
        if cls.__instance == None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def consume_message(self, consume_msg_list=[]):
        """
        消费单个消息并返回
        :param consume_msg_list:
        :return:
        """
        try:
            while True:
                consumer_obj = self._consumer_obj
                logger.info(consumer_obj)
                consumer_obj.subscribe(topics=(self._topic_name))
                records = consumer_obj.poll(max_records=1)
                for key, value in records.items():
                    consume_msg_list.append(value)
                consumer_obj.commit_async()
                if len(consume_msg_list)>0:
                    break
            return consume_msg_list
        except:
            logger.error(traceback.format_exc())


class KafkaClientProducer(object):

    def init_property(self, ):
        """
        #生产者 初始化配置
        :return:
        """
        if not (hasattr(self, '_props') and self._props):
            props = {}
            props['bootstrap_servers'] = settings.KAFKA_CLUSTER
            props['acks'] = settings.KAFKA_ACKS
            props['retries'] = settings.KAFKA_RETRIES
            props['batch_size'] = settings.KAFKA_BATCH_SIZE
            # props['key_serializer'] = settings.KAFKA_KEY_SERIALIZER
            # props['value_serializer'] = settings.KAFKA_VALUE_SERIALIZER
            print props
            self._props = props

    def __init__(self):
        self.init_property()
        self._p_instance = KafkaProducer(**self._props)


    def product_message(self, str_topic_name, str_message):
        """
        生产单个消息并返回
        :param consume_msg_list:
        :return:
        """
        try:
            p_instance = self._p_instance
            p_instance.send(str_topic_name,bytes(str_message))
            # result = future.get(timeout=1)
            # return result
        except:
            logger.error(traceback.format_exc())

if __name__ == '__main__':
    p_instance = KafkaClientProducer()
    p_instance = KafkaClientProducer()
    result = p_instance.product_message('Google_POI_Test', 'so121212me_12121wojiu buyiyang d yanhuo1212121')
    c_instance = KafkaClientConsumer('Google_POI_Test')
    c_instance = KafkaClientConsumer('Google_POI_Test')
    result = c_instance.consume_message()
    print result

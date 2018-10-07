# -*- coding=utf-8 -*-
import gevent
import requests
import traceback
import settings
import os
import sys

from poi_server.PoiHttpHandler import PoiHttpHandler
from poi_server.SearchServerManager import SearchServerManager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import redis
import time
import config
import ujson as json
from public.logger import simple_log
from werkzeug.contrib.fixers import ProxyFix
from flask import Flask, request, make_response
from public.tools import contains_chinese, if_need_filter_poi_item_data_by_lat_lng
from poi_server.KafkaClientConsumer import KafkaClientConsumer





app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)

logger = simple_log()

# 初始化codis集群
redis_handler = redis.Redis(settings.CODIS_SERVER_HOST, settings.CODIS_SERVER_PORT,
                            password=settings.CODIS_SERVER_PASSWORD)


# String strReqUrl,HashMap<String, String> reqParamsMap,String strReqPath
def handle_sear_server_request(str_req_url, str_req_path, req_params_map={}):
    """
    向搜索服务发送请求
    :param str_req_url:
    :param req_params_map: 请求参数
    :param str_req_path: 请求地址
    :return:
    """
    try:
        search_server_ip = settings.SEARCH_SERVER_IP
        search_server_port = settings.SEARCH_SERVER_PORT
        str_url = 'http://%s:%s%s?channel=poi' % (search_server_ip, search_server_port, str_req_path)
        if str_req_url:
            str_url += '&%s' % str_req_url
        else:
            for key, value in req_params_map.iteritems():
                str_url += '&%s=%s' % (key, value)
        response = requests.request('GET', str_url, timeout=20000)
        status = response.status_code
        if status == 200:
            return json.loads(response.text)
        else:
            logger.error('request url get error code:%s url:%d' % (status, str_req_url));
    except:
        logger.error(traceback.format_exc())


def analyze_poi_data(search_result_json, str_poi_data):
    """
    对POI返回的数据进行处理 、合并到搜索结果
    :param search_result_json:
    :param str_poi_data:
    """
    try:
        poi_result_json_array = json.loads(str_poi_data)
        hits_data_array = search_result_json.get('hits')
        hits_poi_object = {}
        hits_poi_array = []
        # 遍历POI数据判断是否有和搜索结果重复的
        for a_poi_resule in poi_result_json_array:
            str_poi_name = a_poi_resule.get('poi_name')
            flng = float(a_poi_resule.get('lng'))
            flat = float(a_poi_resule.get('lat'))
            i_city_id = a_poi_resule.get('city_id')
            str_status = search_result_json.get('status')
            if str_status.upper().__eq__('OK'):
                b_need_filter_search_item = False
                for a_hits_data in hits_data_array:
                    str_type_key = ''
                    if a_hits_data.has_key('attraction'):
                        str_type_key = 'attraction'
                    elif a_hits_data.has_key('area'):
                        str_type_key = 'area'
                    elif a_hits_data.has_key('region'):
                        str_type_key = 'region'
                    elif a_hits_data.has_key('island'):
                        str_type_key = 'island'
                    elif a_hits_data.has_key('country'):
                        str_type_key = 'country'
                    elif a_hits_data.has_key('hotel'):
                        str_type_key = 'hotel'
                    elif a_hits_data.has_key('city'):
                        str_type_key = 'city'
                    if str_type_key:
                        hotel_array = a_hits_data.get(str_type_key)
                        for hotel in hotel_array:
                            i_search_item_city_id = hotel.get('city_id')
                            f_search_item_lng = float(hotel.get('longitude'))
                            f_search_item_lat = float(hotel.get('latitude'))
                            str_search_cn_name = hotel.get('cn_name')

                            if int(i_city_id) == int(i_search_item_city_id):
                                b_need_filter_search_item = if_need_filter_poi_item_data_by_lat_lng(flng, flat,
                                                                                                    f_search_item_lng,
                                                                                                    f_search_item_lat,
                                                                                                    str_poi_name,
                                                                                                    str_search_cn_name,
                                                                                                    str_type_key)
                                if b_need_filter_search_item:
                                    break
                    if b_need_filter_search_item:
                        break
                    if not b_need_filter_search_item:
                        poi_item = {}
                        poi_item['cn_name'] = str_poi_name
                        poi_item['en_name'] = ''
                        poi_item['longitude'] = flng
                        poi_item['latitude'] = flat
                        poi_item['city_id'] = i_city_id
                        poi_item['city'] = a_poi_resule.get('city')
                        poi_item['region'] = a_poi_resule.get('region')
                        poi_item['country'] = a_poi_resule.get('country')
                        poi_item['en_city'] = a_poi_resule.get('en_city')
                        poi_item['area'] = a_poi_resule.get('area')
                        poi_item['en_region'] = a_poi_resule.get('en_region')
                        poi_item['en_area'] = a_poi_resule.get('en_area')
                        poi_item['en_country'] = a_poi_resule.get('en_country')
                        poi_item['is_alias'] = a_poi_resule.get('is_alias')
                        poi_item['is_area'] = a_poi_resule.get('is_area')
                        poi_item['url_name'] = a_poi_resule.get('url_name')
                        poi_item['poi_channel'] = a_poi_resule.get('poi_channel')
                        poi_item['hit_range'] = a_poi_resule.get('hit_range')
                        hits_poi_array.append(poi_item)
        if len(hits_poi_array) > 0:
            hits_poi_object['poi'] = hits_poi_array
            hits_data_array.append(hits_poi_object)
            search_result_json['hits'] = hits_data_array
        search_result_json['is_complete'] = True
    except:
        msg = traceback.format_exc()
        print (msg)
        logger.error(msg)
        return False


def merge_search_result_and_poi_result(search_result_json, req_params_map):
    """
    合并搜索结果 和 poi请求结果
    :param search_result_json:  搜索服务返回结果
    :param req_params_map: 请求搜索服务查询条件
    """
    try:
        str_search_query_word = req_params_map.get('q','')
        str_redis_key = settings.REDIS_POI_SEARCH_HEARD + str_search_query_word
        str_amap_topic_name = settings.AMAP_TOPIC_NAME
        str_google_topic_name = settings.GOOGLE_TOPIC_NAME
        str_poi_status = ''
        # key 存在
        if redis_handler.exists(str_redis_key):
            str_poi_status = redis_handler.hget(str_redis_key, 'status')
            # key存在不需要等待
        else:
            # key不存在
            # 先设置redis key
            redis_handler.hset(str_redis_key, 'status', 'sending')
            # 12小时过期时间
            redis_handler.expire(str_redis_key, 60 * 60 * 12)
            lstart_time = time.time()
            kafka_handker = KafkaClientConsumer.get_consumer_instance()
            # 高德 协程获取结果 并合并到redis  不需要返回结果
            # kafka_handker.consume_message(str_amap_topic_name, str_search_query_word)
            SearchServerManager.asyn_amap_poi_redis(req_params_map)
            lamap_end_time = time.time()
            logger.info('QueryWord:%s produce kafka topic: %s done,time cost: %f ms' % (
                str_search_query_word, str_amap_topic_name, lamap_end_time - lstart_time))
            # 谷歌 协程获取结果 并合并到redis  不需要返回结果
            # kafka_handker.consume_message(str_google_topic_name, str_search_query_word)
            SearchServerManager.asyn_google_poi_redis(req_params_map)
            lgoogle_end_time = time.time()
            logger.info('QueryWord:%s produce kafka topic: %s done,time cost: %f ms' % (
                str_search_query_word, str_google_topic_name, lgoogle_end_time - lamap_end_time))
            for i in range(10):
                gevent.sleep(0.02)
                b_redis_key_exists = redis_handler.exists(str_redis_key)
                if b_redis_key_exists:
                    str_poi_status = redis_handler.hget(str_redis_key, 'status')
                    if str_poi_status.upper().__eq__('OK') or str_poi_status.upper().__eq__('FAIL'):
                        break
        if str_poi_status.upper().__eq__('OK'):
            # 说明 POI已经处理好
            str_poi_data = redis_handler.hget(str_redis_key, 'data')
            # 合并数据处理
            analyze_poi_data(search_result_json, str_poi_data)
        elif str_poi_status.upper().__eq__('FAIL'):
            # search_result_json.put("is_complete", true);
            # 逻辑有问题 - google或 amap一方返回失败状态就置为true，暂时注释掉用于测试，考虑如何完善
            logger.warn('queryWord:%s get poiData fail!' % str_search_query_word);
        else:
            logger.warn('queryWord:%s has not got the PoiData!' % str_search_query_word);
    except:
        logger.error(traceback.format_exc())


def handle_compare_request(req_params_map, str_req_path):
    """
    处理搜索请求
    :param req_params_map:参数
    :param str_req_path: 请求地址
    :return:
    """
    str_rsp_content = '';
    try:
        # 请求好巧搜索服务
        l_start_time = time.time()
        search_result_json = handle_sear_server_request(None, str_req_path, req_params_map=req_params_map)
        str_query_type = req_params_map.get('t', '')
        str_search_query_word = req_params_map.get('q', '')

        search_result_json['is_complete'] = False
        search_result_json['queryWord'] = str_search_query_word
        search_result_json['stamp'] = req_params_map.get('stamp', '')
        is_full_chinese = contains_chinese(str_search_query_word)
        if ((is_full_chinese and len(str_search_query_word) >= 2) or (
                not is_full_chinese and len(
            str_search_query_word) >= 4)) and str_query_type == 'tips' and not req_params_map.get('type', '') and len(
            str_search_query_word) <= 50:
            # 需要查询 POI
            merge_search_result_and_poi_result(search_result_json, req_params_map)
        else:
            search_result_json['is_complete'] = True
        l_merge_end_time = time.time()
        logger.info(('handle request:%s cost: %f ms ')%(json.dumps(req_params_map),l_merge_end_time-l_start_time))
        return search_result_json


    except:
        logger.error(traceback.format_exc())





# 初始化服务
def init_app(app):
    try:
        # 设置utf-8环境
        reload(sys)
        sys.setdefaultencoding('utf-8')

        app.logger.setLevel(config.APP_LOG_LEVEL)
        app.logger.addHandler(logger)

        amap_topic_name = settings.AMAP_TOPIC_NAME
        google_topic_name = settings.GOOGLE_TOPIC_NAME
        # 开启5个高德消费协程
        # for i in range(5):
        #     g_instance = KafkaClientConsumer(amap_topic_name)
        #     gevent.spawn(g_instance.consume_message)
        # # 开启10个谷歌消费协程
        # for i in range(10):
        #     g_instance = KafkaClientConsumer(google_topic_name)
        #     gevent.spawn(g_instance.consume_message)
        # # 这里的等待是协程的等待  还是线程的等待
        # # 延时一会儿，用于kafka消费准备
        # time.sleep(15)
        logger.info('Server is listening on port:%s' % settings.PORT);

    except:
        # 启动失败退出进程
        msg = traceback.format_exc()
        print (msg)
        logger.error(msg)
        sys.exit(1)


# 初始化应用程序
init_app(app)


@app.route('/s')
def poi_search():
    # session_id = Tools.create_session_id()
    try:
        # 解析数据
        request_dict = request.args.to_dict()
        # 获取请求地址
        path = request.url_rule
        # 处理请求
        result_dict = handle_compare_request(request_dict, path)
        # 返回操作结果
        result_string = json.dumps(result_dict,ensure_ascii=False)
        response = make_response(result_string)
        return response
    except:
        logger.error('update_hotels except[%s]' % traceback.format_exc())
        return 'error'


if __name__ == "__main__":
    app.run(host=settings.HOST, port=settings.PORT)

# coding: utf-8
import requests
import traceback
import ujson as json
import settings
from poi_server.PoiHttpHandler import PoiHttpHandler
from public.logger import simple_log
import time
logger = simple_log()


class SearchServerManager(object):
    def handle_sear_server_request(self, str_req_url,str_req_path,req_params_map={}):
        """
        向搜索服务发送请求
        :param str_req_url:
        :param req_params_map:
        :param str_req_path:
        :return:
        """
        try:
            search_server_ip = settings.SEARCH_SERVER_IP
            search_server_port = settings.SEARCH_SERVER_PORT
            str_url = 'http://%s:%s%s?channel=poi'%(search_server_ip,search_server_port,str_req_path)
            if str_req_url:
                str_url+='&%s'%str_req_url
            else:
                for key,value in req_params_map.iteritems():
                    str_req_url+='&%s=%s'%(key,value)
            response = requests.request('GET', str_req_url, timeout=20000)
            status = response.status_code
            if status == 200:
                return  response.text
            else:
                logger.error('request url get error code:%s url:%d'%(status,str_req_url));
        except:
            logger.error(traceback.format_exc())


    def asyn_poi_redis(self,str_poi_channel,req_params_map):
        l_begin_time = time.time()
        if str_poi_channel is 'amap':
            str_amap_result = PoiHttpHandler.handle_amap_request(req_params_map)
            amap_result_json = json.dumps(str_amap_result)
            amap_poi_array = amap_result_json.get('pois',[])
            str_query_word = req_params_map.get('q','')
            for amap_poi in amap_poi_array:
                str_type_code = amap_poi.get('typecode')
                str_poi_name = amap_poi.get('name')
                if (str_type_code.equals('190104') or  str_type_code.equals('190103') or str_type_code.equals('190102')or str_type_code.equals('190101') or str_type_code.equals('100000') or str_type_code.equals('220000') or str_type_code.equals('970000')):
                    logger.warn('queryWord:' + str_query_word + ',\tamap item is filterd due to typeCode:' + str_type_code + '\tpoi_name:' + 'amap');
                else:
                    amap_result_item = {}
                    f_lng = float(amap_poi.get('location').split(',')[0])
                    f_lat = float(amap_poi.get('location').split(',')[1])
                    amap_result_item['poi_name'] = str_poi_name
                    amap_result_item['lng'] = f_lng
                    amap_result_item['lat'] = f_lat
                    amap_result_item['poi_city_name'] = amap_poi.get('cityname')
                    amap_result_item['poi_channel'] = amap_poi.get('amap')
                    hit_range = {}
                    i_Hit_begin = str_poi_name.rfind(str_query_word)
                    if i_Hit_begin != -1:
                        hit_range['start'] = i_Hit_begin
                        hit_range['end'] = len(str_query_word)+i_Hit_begin
                    amap_result_item['hit_range'] = hit_range

        pass

    def asyn_google_poi_redis(self,req_params_map):
        str_google_result = PoiHttpHandler.handle_google_request(req_params_map)
        pass


































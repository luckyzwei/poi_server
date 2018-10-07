# coding: utf-8
import requests
import traceback
import ujson as json
import settings
from public.logger import simple_log
import urllib

from public.static import search_cxds

logger = simple_log()


class PoiHttpHandler(object):

    def handle_amap_request(self, client_req_params_map):
        """
        ## 处理高德地图请求
        :param client_req_params_map:
        :return:
        """
        try:
            str_req_key = settings.AMAP_REQ_KEY
            str_req_url = settings.AMAP_REQ_BASE_URL
            keywords = client_req_params_map.get('q', '')
            req_url = str_req_url + 'key=%s&keywords=%s' % (str_req_key, urllib.quote(keywords))
            response = requests.request('GET', req_url, timeout=20000)
            status = response.status_code
            if status == 200:
                return  response.text
            else:
                logger.error(('request url get error code:%s url:%d'%(status,req_url)));
        except:
            logger.error(traceback.format_exc())

    def handle_google_request(self,client_req_params_map):
        """
        处理google地图请求
        :param str_req_type: 请求类型
                            textsearch-地点搜索
                            details-地点详情
        :return:
        """
        try:
            if settings.DEV:
                return json.dumps(search_cxds.google_poi_cxds)
            else:
                str_req_key = settings.GOOGLE_REQ_KEY
                str_req_url = settings.GOOGLE_REQ_BASE_URL
                keywords = client_req_params_map.get('keywords', '')
                language = client_req_params_map.get('language', 'zh-CN')
                str_req_type = client_req_params_map.get('strReqType', 'textsearch')
                req_url = str_req_url + 'key=%s&query=%s&language=%s' % (str_req_type,str_req_key, urllib.quote(keywords),language)
                response = requests.request('GET', req_url, timeout=20000,proxies=settings.PROXIES)
                status = response.status_code
                if status == 200:
                    return  response.text
                else:
                    logger.error(('request url get error code:%s url:%d'%(status,req_url)));
        except:
            logger.error(traceback.format_exc())

























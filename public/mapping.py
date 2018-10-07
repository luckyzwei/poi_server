#!/usr/bin/env python
# -*- coding=utf-8 -*-
"""
    负责酒店映射服务的调用
"""

import sys
import traceback
import requests
sys.path.append('../')
from public.consts import Const

# 酒店映射服务
# H2S_MAP_SERVER_HEAD = 'http://10.25.60.51:54561/h2s?service_channel=4&hotel=%s'     # format hq_hotel_id
H2S_MAP_SERVER_HEAD = 'http://mask-control.haoqiao.com:50000/h2s?show_zombie=1&service_channel=4&hotel=%s'     # format hq_hotel_id


class Mapping(object):

    @classmethod
    def get_good_supplier(cls, hq_hotel_id, max_diffuse_num=0, include_supplier=0, logger=None):
        """
        获取此酒店的优选供应商酒店列表
        :param hq_hotel_id:
        :param max_diffuse_num
        :param include_supplier:
        :return: list()
        """
        try:
            url = H2S_MAP_SERVER_HEAD % hq_hotel_id
            ret = requests.get(url, 30)
            ret = ret.json()
            export = ret[Const.data][Const.Export]
            hq_city_id = ret[Const.data][Const.hq_city_id]
            hotel_dict = dict()
            supplier_weight = dict()
            for item in export:
                try:
                    sp_city_code = item[Const.sp_city_code]
                    sp_city_name = item[Const.sp_city_name]
                    sp_hotel_code = item[Const.sp_hotel_code]
                    supplier_id = int(item[Const.supplier_id])
                    if supplier_id not in hotel_dict:
                        hotel_dict[supplier_id] = dict()
                    hotel_dict[supplier_id][sp_hotel_code] = (supplier_id, hq_hotel_id, hq_city_id, sp_hotel_code, sp_city_code, sp_city_name)
                except:
                    logger.error(traceback.format_exc())
            if max_diffuse_num > 0:
                sorted_supplier_list = sorted(supplier_weight.items(), key=lambda d:d[1], reverse=True)[:max_diffuse_num]
            else:
                sorted_supplier_list = sorted(supplier_weight.items(), key=lambda d:d[1], reverse=True)
            sorted_list = list()
            for supplier_id_i, weight_i in sorted_supplier_list:
                for sp_hotel_code, pair in hotel_dict[supplier_id_i].items():
                    sorted_list.append(pair)
            return sorted_list
        except:
            logger.error(traceback.format_exc())
            return list()

    @classmethod
    def get_supplier_info(cls, supplier_id, hq_hotel_id, logger=None, need_supplier_id=0):
        """
        根据好巧酒店信息获取供应商酒店信息
        :param hq_hotel_id:
        :param supplier_id:
        :return:
        """
        info = list()
        url = 'http://mask-control.haoqiao.com:50000/h2s?service_channel=4&hotel=%s&supplier_id=%s' % (hq_hotel_id, supplier_id)
        try:
            ret = requests.get(url, 30)
            ret = ret.json()
            export = ret[Const.data][Const.Export]
            hq_city_id = ret[Const.data][Const.hq_city_id]
            for item in export:
                try:
                    sp_city_code = item[Const.sp_city_code]
                    sp_city_name = item[Const.sp_city_name]
                    sp_hotel_code = item[Const.sp_hotel_code]
                    supplier_id = int(item[Const.supplier_id])
                    pair = (supplier_id, hq_hotel_id, hq_city_id, sp_hotel_code, sp_city_name, sp_city_code)
                    if need_supplier_id == 0:
                        info.append(pair)
                    elif str(supplier_id) == need_supplier_id:
                        info.append(pair)
                except:
                    logger.error(traceback.format_exc())
            return info
        except:
            logger.error('url=%s trace=%s' % (traceback.format_exc(), url))
            return dict()

    @classmethod
    def get_all_hotel_info(cls, hq_hotel_id, logger):
        """
        获取此酒店的优选供应商酒店列表
        :param hq_hotel_id:
        :param logger
        :return: list()
        """
        try:
            url = H2S_MAP_SERVER_HEAD % hq_hotel_id
            ret = requests.get(url, 30)
            ret = ret.json()
            export = ret[Const.data][Const.Export]
            hq_city_id = ret[Const.data][Const.hq_city_id]
            result = set()
            for item in export:
                try:
                    sp_city_code = item[Const.sp_city_code]
                    sp_city_name = item[Const.sp_city_name]
                    sp_hotel_code = item[Const.sp_hotel_code]
                    supplier_id = int(item[Const.supplier_id])
                    result.add((supplier_id, hq_hotel_id, sp_hotel_code, sp_city_code, sp_city_name, hq_city_id))
                except:
                    logger.error(traceback.format_exc())
            return result
        except:
            logger.error(traceback.format_exc())
            return set()

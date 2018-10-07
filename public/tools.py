#!/usr/bin/env python
# -*- coding=utf-8 -*-
import math
import re
import traceback
import logger
from public.logger import simple_log

logger = simple_log()


def contains_chinese(contents):
    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
    match = zhPattern.search(contents)
    if match:
        return True
    else:
        return False


def to_radians_float(degress):
    return degress * math.pi / 180.0


def get_poi_distance(f_poi_lng, f_poi_lat, f_search_data_lng, f_search_data_lat):
    """
    获取两个poi点之间的直线距离
    :param f_poi_lng:
    :param f_poi_lat:
    :param f_search_data_lng:
    :param f_search_data_lat:
    """
    fdlng = to_radians_float(f_poi_lng - f_search_data_lng)
    fdlat = to_radians_float(f_poi_lat - f_search_data_lat)
    a = math.sin(fdlat) * math.sin(fdlat) + math.cos(to_radians_float(f_poi_lat)) * math.cos(
        to_radians_float(f_search_data_lat)) * math.sin(fdlng / 2) * math.sin(fdlng / 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    EARCH_RADIUS = 6378137.0
    return EARCH_RADIUS * c


def if_need_filter_poi_item_data_by_lat_lng(f_poi_lng, f_poi_lat, f_search_data_lng, f_search_data_lat, str_poi_name,
                                            str_search_cn_name, str_type_key):
    """
     过滤 符合规则的poi数据，暂定规则：
	 1:POI返回的数据和搜索数据直线距离<=500m且名称3/4重合
	 2:POI返回的数据和搜索数据直线距离<=1400m且名称完全相同
    :param f_poi_lng:
    :param f_poi_lat:
    :param f_search_data_lng:
    :param f_search_data_lat:
    :param str_poi_name:
    :param str_search_cn_name:
    :param str_type_key:
    :return:
    """
    try:
        f_distance = get_poi_distance(f_poi_lng, f_poi_lat, f_search_data_lng, f_search_data_lat)

        b_name_match = False
        b_full_math = False
        i_poi_name_len = len(str_poi_name)
        if i_poi_name_len < 4:

            b_name_match = str_search_cn_name.find(str_poi_name) != -1
        else:
            b_name_match = str_search_cn_name.find(str_poi_name[:i_poi_name_len * 3 / 4]) != -1
        b_full_math = str_search_cn_name.__eq__(str_poi_name)
        if f_distance <= 500 and b_name_match:
            logger.warn(
                'radius distance less than 500m between poi item and search item! radius is: %d \tpoi info is:[%d,%d,%d,%d] \tpoi_name:%s,search_cn_name:%s,searchType:%s') % \
            (f_distance,f_poi_lng,f_poi_lat,f_search_data_lng,f_search_data_lat,str_poi_name,str_search_cn_name,str_type_key);
            return True
        elif f_distance<=1400 and b_full_math:
            logger.warn(
                'radius distance less than 1400m between poi item and search item! radius is: %d \tpoi info is:[%d,%d,%d,%d] \tpoi_name:%s,search_cn_name:%s,searchType:%s') % \
            (f_distance, f_poi_lng, f_poi_lat, f_search_data_lng, f_search_data_lat, str_poi_name, str_search_cn_name,
             str_type_key);
            return True
        return False


    except:
        logger.error(traceback.format_exc())

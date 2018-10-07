#!/usr/bin/env python
# -*- coding=utf-8 -*-
from public.consts import Const
from public.pymysql_adapter import *


def get_hotel_order_supplier(hq_hotel_id):
    """
    获取好巧酒店半年内成单供应商集合
    :param hq_hotel_id:
    :return:  set()
    """
    try:
        import datetime
        supplier_set = set()
        start_day = datetime.date.today() + datetime.timedelta(days=-30*6)
        sql = "select supplier_id, count(*) as c from hqs_order where order_id in (select order_id from hqs_order_hotel " \
              "where hotel_id=%s) and create_time>'%s' " \
              "GROUP BY supplier_id ORDER BY c DESC;" % (hq_hotel_id, str(start_day))
        ret = pymysql_query(sql, Const.orders)
        if ret == DB_EXCEPTION:
            return set()
        for row in ret:
            try:
                supplier_id = row[Const.supplier_id]
                supplier_set.add(supplier_id)
            except:
                continue
        return supplier_set
    except:
        return set()


def get_test_hotels():
    """
    获取测试酒店, 比较对比效果
    :return:
    """
    try:
        tmp_hq_hotel_set = set()
        sql = "select hotel_id as hq_hotel_id, count(hotel_id) as c " \
              "FROM hqs_order_hotel WHERE order_id in " \
              "(select order_id from hqs_order where create_time>'2017-12-23' and create_time < '2018-01-22') " \
              "GROUP BY hotel_id order by c DESC limit 100;"
        ret = pymysql_query(sql, Const.orders)
        if ret == DB_EXCEPTION:
            return set()
        for row in ret:
            try:
                hq_hotel_id = row[Const.hq_hotel_id]
                tmp_hq_hotel_set.add(hq_hotel_id)
            except:
                continue
        return tmp_hq_hotel_set
    except:
        return set()


def get_compared_supplier(hq_hotel_id, limit=5):
    """
    获取此酒店比价后的优势供应商,在regular及min的前n个的交集里去limit个
    :param hq_hotel_id:
    :param limit: 前 limit 个
    :return: set()
    """
    try:
        sql_regular = "SELECT hq_hotel_id, supplier_id, regular_rooms_score, min_price_score " \
              "FROM hotel_supplier_rank WHERE hq_hotel_id=%s GROUP BY hq_hotel_id, supplier_id " \
              "ORDER BY regular_rooms_score desc LIMIT 5" % hq_hotel_id
        regular_supplier = set()
        ret = pymysql_query(sql_regular, Const.schedule)
        for row in ret:
            try:
                supplier_id = row[Const.supplier_id]
                regular_supplier.add(supplier_id)
            except:
                continue
        min_supplier = set()
        sql_min = "SELECT hq_hotel_id, supplier_id, regular_rooms_score, min_price_score " \
                  "FROM hotel_supplier_rank WHERE hq_hotel_id=%s GROUP BY hq_hotel_id, supplier_id " \
                  "ORDER BY min_price_score desc LIMIT 5" % hq_hotel_id
        ret = pymysql_query(sql_min, Const.schedule)
        for row in ret:
            try:
                supplier_id = row[Const.supplier_id]
                min_supplier.add(supplier_id)
            except:
                continue
        return regular_supplier, min_supplier
    except:
        return set(), set()


def check_result(hq_hotel_set):
    """
    对比下比较后的这批酒店, 在历史上的成单供应商是否集中在比价结果所挑选出来的酒店
    :param hq_hotel_set:
    :return:
    """
    try:
        """每个好巧酒店的结果, 比价出的供应商集合, 历史订单出的酒店集合, 交集占订单供应商集合比"""
        f = open('result.txt', 'w+')
        for hq_hotel_id in hq_hotel_set:
            try:
                f.write('######## %s #########\n' % hq_hotel_id)
                ordered_supplier_set = get_hotel_order_supplier(hq_hotel_id)
                f.write('产生订单的供应商:%s\n' % ordered_supplier_set)
                regular_supplier, min_price_supplier = get_compared_supplier(hq_hotel_id, 5)
                f.write('规整房型优选的供应商:%s\n' % regular_supplier)
                f.write('最低价优选的供应商:%s\n' % min_price_supplier)
                # 交集
                mix = ordered_supplier_set & regular_supplier
                rate = len(mix) * 1.0 / len(regular_supplier)
                f.write('规整房型优选供应商交集占订单供应商比例:%.2f\n' % rate)
                mix = ordered_supplier_set & min_price_supplier
                rate = len(mix) * 1.0 / len(min_price_supplier)
                f.write('最低价优选供应商交集占订单供应商比例:%.2f\n' % rate)
            except:
                continue
        f.close()
        return
    except:
        return

#!/usr/bin/env python
# -*- coding=utf-8 -*-

import sys
import datetime
import config
sys.path.append('../')
from public.tools import Tools
from public.consts import Const
from public.pymysql_adapter import *


class Picker(object):
    """

    """
    def __init__(self, logger):
        self.logger = logger

    def pick(self):
        """
        根据策略挑选一批好巧酒店
        :return:
        """
        try:
            if config.ORDER_FLAG:
                ordered_hotels = self.pick_ordered_hotels()
            else:
                ordered_hotels = set()
            interested_hotels = self.pick_interested_hotels()
            # 任务表中记录的待处理任务
            task_hotels = self.pick_task_hotels()
            omit_hotels = self.need_omit_hotels()
            hotels = ordered_hotels | interested_hotels | task_hotels - omit_hotels
            return hotels
        except:
            self.logger.error(traceback.format_exc())
            return set()

    def pick_from_task_table(self):
        """
        从配置表中获取任务
        :return:
        """
        try:
            hotel_set = set()
            special_hotel = set()
            sql = "SELECT hq_hotel_id, task_type FROM compare_hotel_task WHERE status=0"
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error(traceback.format_exc())

            for row in ret:
                hq_hotel_id = row[Const.hq_hotel_id]
                task_type = row[Const.task_type]
                if task_type == 3:
                    special_hotel.add(hq_hotel_id)
                hotel_set.add(hq_hotel_id)
            omit_hotels = self.need_omit_hotels()
            return (hotel_set - omit_hotels) | special_hotel
        except:
            self.logger.error(traceback.format_exc())
            return set()

    def pick_task_hotels(self):
        """
        从任务表中获取任务
        :return:
        """
        try:
            now = datetime.datetime.now()
            task_hotels_set = set()
            sql = "SELECT DISTINCT hq_hotel_id FROM compare_hotel_task WHERE status=1 AND create_time<'%s'" % now
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error(traceback.format_exc())
            for row in ret:
                hq_hotel_id = row[Const.hq_hotel_id]
                task_hotels_set.add(hq_hotel_id)
            # 清理任务
            sql_update = "UPDATE compare_hotel_task SET status=0 WHERE create_time < '%s'" % now
            ret = pymysql_query(sql_update, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error(traceback.format_exc())
            return task_hotels_set
        except:
            self.logger.error(traceback.format_exc())
            return set()

    def pick_interested_hotels(self, limit=2000):
        """
        选取用户感兴趣好巧酒店, 30天以内穿透量最多的limit家好巧酒店
        :param limit: 吐出酒店数量上限
        :return: set
        """
        try:
            interested_hotels_info = dict()
            download_suppliers = Tools.get_dls_suppliers()
            if not download_suppliers:
                return set()
            day = datetime.datetime.today() - datetime.timedelta(days=30)
            for supplier_id in download_suppliers:
                try:
                    sql = "SELECT hq_hotel_id, price_num FROM summary%s WHERE stat_date >= '%s' " \
                          "ORDER BY price_num DESC limit %s;" % (supplier_id, day, limit)
                    ret = pymysql_query(sql, Const.summary)
                    if ret == DB_EXCEPTION:
                        self.logger.error(traceback.format_exc())
                        continue
                    for row in ret:
                        hq_hotel_id = row[Const.hq_hotel_id]
                        price_num = row[Const.price_num]
                        if price_num == 0:
                            break
                        if hq_hotel_id not in interested_hotels_info:
                            interested_hotels_info[hq_hotel_id] = price_num
                        else:
                            interested_hotels_info[hq_hotel_id] += price_num
                except:
                    self.logger.error(traceback.format_exc())
            # 根据最大请求数降序排序
            sorted_hotels = sorted(interested_hotels_info.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
            interested_hotels = set()
            for item in sorted_hotels[:limit]:
                hq_hotel_id, num = item
                interested_hotels.add(hq_hotel_id)
            return interested_hotels
        except:
            self.logger.error(traceback.format_exc())
            return set()

    def pick_ordered_hotels(self, count=1, before=-1, limit=-1):
        """
        将产生过订单的酒店拿出来
        :param before: 订单系统内多少个月以前的数据, -1代表全部时间
        :param count: 酒店产生订单量
        :param limit: 返回限制总数, -1代表无限制
        :return:
        """
        try:
            ordered_hotels = set()
            today = datetime.datetime.today()
            if before != -1:
                day = today + datetime.timedelta(days=-before*12)
            else:
                day = 0
            if limit != -1:
                limit_str = 'LIMIT %s' % limit
            else:
                limit_str = ''
            sql = "SELECT hotel_id AS hq_hotel_id, count(hotel_id) AS c FROM hqs_order_hotel WHERE order_id IN " \
                  "(SELECT order_id FROM hqs_order WHERE create_time>'%s') " \
                  "GROUP BY hotel_id HAVING c>= %s ORDER BY c DESC %s;" % (day, count, limit_str)
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error('exec sql=%s error' % sql)
                return set()
            self.logger.info('exec sql=[%s] ret=%s' % (sql, len(ret)))
            for row in ret:
                try:
                    hq_hotel_id = row[Const.hq_hotel_id]
                    ordered_hotels.add(hq_hotel_id)
                except:
                    self.logger.error(traceback.format_exc())
            return ordered_hotels
        except:
            self.logger.error(traceback.format_exc())
            return set()

    def need_omit_hotels(self, effect_days=config.COMPARE_EFFECT_DAYS):
        """
        返回需要忽略的好巧酒店, 近期已经比过价的也不需要再比价
        :param effect_days 此日期内更新过的任务不再参与本次比价
        :return:
        """
        try:
            omit_hotels = set()
            day = datetime.datetime.today() - datetime.timedelta(days=effect_days)
            sql = "SELECT DISTINCT hq_hotel_id FROM hotel_supplier_rank WHERE update_time > '%s'" % day
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error('exec sql=%s error' % sql)
                return set()
            self.logger.info('exec sql=[%s] ret=%s' % (sql, len(ret)))
            for row in ret:
                try:
                    hq_hotel_id = row[Const.hq_hotel_id]
                    omit_hotels.add(hq_hotel_id)
                except:
                    self.logger.error(traceback.format_exc())
            return omit_hotels
        except:
            self.logger.error(traceback.format_exc())
            return set()

    def pick_ctrip_online_hotels_info(self, intl=False):
        """
        获取携程上线酒店集合
        :return:
        """
        try:
            hotel_info = dict()
            if intl:
                sql = "SELECT hotel_id as hq_hotel_id,supplier_id,supplier_blacklist,city_id FROM `haoqiao_ctrip_intl_online_hotels` WHERE status=0;"
            else:
                sql = "SELECT hotel_id as hq_hotel_id,supplier_id,supplier_blacklist,city_id FROM `haoqiao_ctrip_online_hotels` WHERE status=0;"
            ret = pymysql_query(sql, Const.ota)
            if ret == DB_EXCEPTION:
                self.logger.error(traceback.format_exc())
                return hotel_info
            self.logger.info('sql=%s ret=%s' % (sql, len(ret)))
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    supplier_id = str(row[Const.supplier_id])
                    city_id = str(row[Const.city_id])
                    supplier_blacklist = row[Const.supplier_blacklist]
                    if hq_hotel_id not in hotel_info:
                        hotel_info[hq_hotel_id] = dict()
                    hotel_info[hq_hotel_id][Const.city_id] = city_id
                    hotel_info[hq_hotel_id][Const.currentsupplier] = supplier_id.split(',')
                    black_list = supplier_blacklist.split(',')
                    if black_list == ['']:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = list()
                    else:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = [int(i) for i in black_list]
                except:
                    self.logger.error(traceback.format_exc())
            return hotel_info
        except:
            self.logger.error(traceback.format_exc())
            return dict()

    def pick_fliggy_online_hotels(self):
        """
        获取携程上线酒店集合
        :return:
        """
        try:
            hotel_info = dict()
            sql = "SELECT hotel_id as hq_hotel_id,supplier_id,supplier_blacklist,city_id FROM `haoqiao_fliggy_online_hotels` WHERE status=0 and confirm_str = '匹配成功';"
            ret = pymysql_query(sql, Const.ota)
            if ret == DB_EXCEPTION:
                self.logger.error(traceback.format_exc())
                return hotel_info
            self.logger.info('sql=%s ret=%s' % (sql, len(ret)))
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    supplier_id = str(row[Const.supplier_id])
                    city_id = str(row[Const.city_id])
                    supplier_blacklist = row[Const.supplier_blacklist]
                    if hq_hotel_id not in hotel_info:
                        hotel_info[hq_hotel_id] = dict()
                    hotel_info[hq_hotel_id][Const.city_id] = city_id
                    hotel_info[hq_hotel_id][Const.currentsupplier] = supplier_id.split(',')
                    black_list = supplier_blacklist.split(',')
                    if black_list == ['']:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = list()
                    else:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = [int(i) for i in black_list]
                except:
                    self.logger.error(traceback.format_exc())
            return hotel_info
        except:
            self.logger.error(traceback.format_exc())
            return dict()

    def pick_qunar_online_hotels(self):
        """

        :return:
        """
        try:
            hotel_info = dict()
            sql = "SELECT hotel_id as hq_hotel_id,supplier_id,supplier_blacklist, city_id FROM `haoqiao_qunar_online_hotels` WHERE status=0;"
            ret = pymysql_query(sql, Const.ota)
            if ret == DB_EXCEPTION:
                self.logger.error(traceback.format_exc())
                return hotel_info
            self.logger.info('sql=%s ret=%s' % (sql, len(ret)))
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    supplier_id = str(row[Const.supplier_id])
                    city_id = str(row[Const.city_id])
                    supplier_blacklist = row[Const.supplier_blacklist]
                    if hq_hotel_id not in hotel_info:
                        hotel_info[hq_hotel_id] = dict()
                    hotel_info[hq_hotel_id][Const.city_id] = city_id
                    hotel_info[hq_hotel_id][Const.currentsupplier] = supplier_id.split(',')
                    black_list = supplier_blacklist.split(',')
                    if black_list == ['']:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = list()
                    else:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = [int(i) for i in black_list]
                except:
                    self.logger.error(traceback.format_exc())
            return hotel_info
        except:
            self.logger.error(traceback.format_exc())
            return dict()

    def pick_qunar_spec_online_hotels(self):
        """

        :return:
        """
        try:
            hotel_info = dict()
            sql = "SELECT hotel_id as hq_hotel_id,supplier_id,supplier_blacklist, city_id FROM `haoqiao_ota_qunar_hotels` WHERE status=0;"
            ret = pymysql_query(sql, Const.ota)
            if ret == DB_EXCEPTION:
                self.logger.error(traceback.format_exc())
                return hotel_info
            self.logger.info('sql=%s ret=%s' % (sql, len(ret)))
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    supplier_id = str(row[Const.supplier_id])
                    city_id = str(row[Const.city_id])
                    supplier_blacklist = row[Const.supplier_blacklist]
                    if hq_hotel_id not in hotel_info:
                        hotel_info[hq_hotel_id] = dict()
                    hotel_info[hq_hotel_id][Const.city_id] = city_id
                    hotel_info[hq_hotel_id][Const.currentsupplier] = supplier_id.split(',')
                    black_list = supplier_blacklist.split(',')
                    if black_list == ['']:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = list()
                    else:
                        hotel_info[hq_hotel_id][Const.supplier_blacklist] = [int(i) for i in black_list]
                except:
                    self.logger.error(traceback.format_exc())
            return hotel_info
        except:
            self.logger.error(traceback.format_exc())
            return dict()


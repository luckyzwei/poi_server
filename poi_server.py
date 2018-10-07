#!/usr/bin/env python
# -*- coding=utf-8 -*-

from gevent import monkey;

monkey.patch_all()
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import traceback
import gevent
import config
import logging
import datetime
import redis
import copy
import ujson as json
from public.consts import Const
from public.pymysql_adapter import *
from public.tools import Tools
from public.logger import Logger
from public.logger import AliLog
from pick.picker import Picker
from compare.comparer import Compare
from flask import Flask, request, make_response
from werkzeug.contrib.fixers import ProxyFix

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler


class CompareServer(object):
    """
    用于控制对比程序
    """

    def __init__(self, logger, redis_handler):
        self.scheduler = BackgroundScheduler()
        self.logger = logger
        self.redis_handler = redis_handler
        self.hq_hotel_id_set = set()
        self.scheduled_supplier = Tools.get_schedule_supplier()
        # self.qunar_online_hotels = None
        # self.qunar_spec_online_hotels = None
        # self.ctrip_online_hotels = None
        # self.fliggy_online_hotels = None
        # ota 定时任务 每周一晚19点跑一次
        self.scheduler.add_job(self.ota_task_sync, 'cron', day_of_week='fri', hour=19)
        self.scheduler.add_job(self.ota_result_sync, 'cron', day_of_week='fri', hour=22)
        self.scheduler.start()

    def run(self):
        # 开启针对ota供应商轮换的比价操作
        # gevent.spawn(self.ota_result_sync)
        # 开启接口接受到的任务到任务表同步的协程
        # gevent.spawn(self.sync_task_worker)
        while True:
            try:
                # 选出待比较的好巧酒店
                # self.hq_hotel_id_set = self.pick_hq_hotels()
                self.hq_hotel_id_set = self.pick_hq_hotels()
                if self.hq_hotel_id_set:
                    self.logger.info('picked %s hotel to compare' % len(self.hq_hotel_id_set))
                    worker = Compare(self.hq_hotel_id_set, self.logger)
                    worker.work()
                # 休眠10分钟
                gevent.sleep(60 * 10)
            except:
                self.logger.error(msg=traceback.format_exc())

    def ota_task_sync(self):
        """
        负责处理针对ota的供应商轮换操作
        :return:
        """
        try:
            self.logger.info('@ota_task_sync start')
            picker = Picker(self.logger)
            # 扫描ota上线酒店表将需要比价的酒店放入比价任务队列
            if config.OTA_FLIPPY_STATUS:
                fliggy_online_hotels = picker.pick_fliggy_online_hotels()
                send_task_to_table(fliggy_online_hotels.keys(), 'fliggy', config.TYPE_ONE)
            if config.OTA_CTRIP_STATUS:
                ctrip_online_hotels = picker.pick_ctrip_online_hotels_info()
                send_task_to_table(ctrip_online_hotels.keys(), 'ctrip', config.TYPE_ONE)
            if config.OTA_QUNAR_STATUS:
                qunar_online_hotels = picker.pick_qunar_online_hotels()
                send_task_to_table(qunar_online_hotels.keys(), 'qunar', config.TYPE_ONE)
            self.logger.info('@ota_task_sync end')
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def ota_result_sync(self):
        """
        用于同步酒店优选供应商的结果
        :return:
        """
        try:
            self.logger.info('@ota_result_sync start')
            picker = Picker(self.logger)
            if config.OTA_FLIPPY_STATUS:
                self.logger.info('fliggy start ota_result_sync ')
                # 选取要轮换酒店
                fliggy_online_hotels = picker.pick_fliggy_online_hotels()
                # 更新轮换酒店供应商数据
                self.update_fliggy_hotel_supplier(fliggy_online_hotels)
                # 上线此分销商调度任务
                Tools.ota_hotels('haoqiao_fliggy_online_hotels', 206)
            if config.OTA_CTRIP_STATUS:
                # 选取要轮换酒店
                self.logger.info('ctrip start ota_result_sync ')
                ctrip_online_hotels = picker.pick_ctrip_online_hotels_info()
                # 更新轮换酒店供应商数据
                self.update_ctrip_hotel_supplier(ctrip_online_hotels)
                # 上线此分销商调度任务
                Tools.ota_hotels('haoqiao_ctrip_online_hotels', 205)
            if config.OTA_CTRIP_INTL_STATUS:
                # 选取要轮换酒店
                self.logger.info('ctrip start ota_result_sync ')
                ctrip_online_hotels = picker.pick_ctrip_online_hotels_info(intl=True)
                # 更新轮换酒店供应商数据
                self.update_ctrip_hotel_supplier(ctrip_online_hotels, intl=True)
                # 上线此分销商调度任务
                Tools.ota_hotels('haoqiao_ctrip_intl_online_hotels', 204)
            if config.OTA_QUNAR_STATUS:
                # 选取要轮换酒店
                self.logger.info('qunar start ota_result_sync ')
                qunar_online_hotels = picker.pick_qunar_online_hotels()
                # 更新轮换酒店供应商数据
                self.update_qunar_hotel_supplier(qunar_online_hotels)
                # 上线此分销商调度任务
                Tools.ota_hotels('haoqiao_qunar_online_hotels', 207)
            if config.OTA_QUNAR_SPEC_STATUS:
                # 选取要轮换酒店
                self.logger.info('qunar_spec start ota_result_sync ')
                qunar_spec_online_hotels = picker.pick_qunar_spec_online_hotels()
                # 更新轮换酒店供应商数据
                self.update_qunar_spec_hotel_supplier(qunar_spec_online_hotels)
                # 上线此分销商调度任务
                Tools.ota_hotels('haoqiao_ota_qunar_hotels', 208)
            self.logger.info('@ota_result_sync end')
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def update_fliggy_hotel_supplier(self, fliggy_online_hotels):
        """
        更新飞猪上线酒店优选供应商
        :return:
        """
        try:
            if len(fliggy_online_hotels.keys()) == 0:
                return True
            day = datetime.datetime.now() - datetime.timedelta(days=30 * 3)
            sql = "SELECT hq_hotel_id, substring_index(group_concat(supplier_id ORDER BY min_price_score DESC),',',10) AS 'top' " \
                  "FROM hotel_supplier_rank WHERE update_time > '%s' AND hq_hotel_id in (%s) GROUP BY hq_hotel_id" % \
                  (day, ','.join(fliggy_online_hotels.keys()))
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error('Exec sql=%s error' % sql)
                return False
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    ordered_suppliers = self.get_hotel_order_suppliers(hq_hotel_id)
                    # 排好序的供应商列表
                    top_list = row[Const.top].split(',')
                    if len(top_list) == 1 and top_list[0] == '0':
                        fliggy_online_hotels[hq_hotel_id][Const.new_suppliers] = list()
                        continue
                    area = Tools.get_hotel_area(hq_hotel_id)
                    if area == -1:
                        self.logger.error('get country error, hq_hotel_id=%s' % hq_hotel_id)
                        continue
                    # 如果酒店所在地区未在影响范围内,忽略 国家数据：104 中国大陆, 107 中国台湾, 216 中国港澳
                    if area in config.OTA_AREA_BLACK_LIST['fliggy']:
                        continue
                    fliggy_online_hotels[hq_hotel_id][Const.top] = top_list
                    fliggy_online_hotels[hq_hotel_id][Const.ordered_suppliers] = ordered_suppliers
                    # 将此酒店此端的黑名单供应商过滤掉,生成可上线的供应商
                    new_supplier_list = self.filter_blacklist('fliggy', area, top_list, ordered_suppliers,
                                                              fliggy_online_hotels[hq_hotel_id][
                                                                  Const.supplier_blacklist])
                    if new_supplier_list == [''] or len(new_supplier_list) == 0:
                        new_supplier_list = fliggy_online_hotels[hq_hotel_id][Const.currentsupplier]
                        self.logger.warn('@hq_hotel_id=%s get new supplier list None, stand by' % hq_hotel_id)
                    fliggy_online_hotels[hq_hotel_id][Const.new_suppliers] = new_supplier_list
                    self.logger.info(
                        '@fliggy hq_hotel_id=%s info=%s' % (hq_hotel_id, fliggy_online_hotels[hq_hotel_id]))
                    gevent.sleep(0.01)
                except:
                    self.logger.error(msg=traceback.format_exc())
            # 更新飞猪上线表供应商列表
            self.update_fliggy_hotel_table(fliggy_online_hotels)
            # 更新日志表
            self.update_supplier_change_logger_table('fliggy', fliggy_online_hotels)
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def update_supplier_change_logger_table(self, source, online_hotels):
        """
        更新变更记录
        :param online_hotels:
        :return:
        """
        try:
            if source == 'fliggy':
                channel = 3
            elif source == 'ctrip':
                channel = 2
            elif source == 'qunar':
                channel = 1
            else:
                return False
            values = list()
            cursor = start_transaction(Const.ota)
            for hq_hotel_id, item in online_hotels.items():
                try:
                    old_list = item[Const.currentsupplier]
                    new_list = item[Const.new_suppliers]
                    if old_list == new_list:
                        continue
                    value = "(%s,%s,'%s','%s',3,'compare','%s')" % (
                        channel, hq_hotel_id, ','.join(old_list), ','.join(new_list), datetime.datetime.now())
                    values.append(value)
                except:
                    self.logger.error(msg=traceback.format_exc())
            sql = "INSERT INTO haoqiao_ota_hotels_suppliers_records(channel,hotel_id,old_supplier_id,supplier_id,op_type,op_user,add_time) " \
                  "VALUES %s" % ','.join(values)
            pymysql_transaction_query(sql, cursor)
            commit_transaction(cursor)
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def update_fliggy_hotel_table(self, fliggy_online_hotels):
        """
        将fliggy_online_hotels中记录的新供应商列表更新到飞猪上线酒店表中
        :return:
        """
        try:
            cursor = start_transaction(Const.ota)
            for hq_hotel_id, item in fliggy_online_hotels.items():
                try:
                    old_list = set(item[Const.currentsupplier])
                    new_list = set(item[Const.new_suppliers])
                    if old_list == new_list:
                        continue
                    sql = "UPDATE haoqiao_fliggy_online_hotels SET supplier_id='%s', add_time=now() WHERE hotel_id=%s" % (
                        ','.join(new_list), hq_hotel_id)
                    pymysql_transaction_query(sql, cursor)
                except:
                    self.logger.error(msg=traceback.format_exc())
            commit_transaction(cursor)
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def filter_blacklist(self, distributor, area, compared_list, ordered_list, hotel_black_list, max_num=5):
        """
        负责筛选此酒店, 此端的黑名单供应商, 评估比价后的列表和出现订单的供应商列表, 最终选出上线的供应商
        :param distributor : str ['ctrip', 'fliggy', 'qunar']
        :param compared_list:
        :param ordered_list:
        :param area 国家码 # 国家数据：104 中国大陆, 107 中国台湾, 216 中国港澳
        :param hotel_black_list, 上线表中记录的酒店黑名单列表
        :param max_num:
        :return:
        """
        try:
            # 黑名单
            mainland_black_list = (config.OTA_BLACK_LIST['mainland'].get(distributor) or list()) + hotel_black_list
            abroad_black_list = (config.OTA_BLACK_LIST['abroad'].get(distributor) or list()) + hotel_black_list
            hks_black_list = (config.OTA_BLACK_LIST['hks'].get(distributor) or list()) + hotel_black_list
            # 白名单
            mainland_white_list = config.OTA_WHITE_LIST['mainland'].get(distributor)
            abroad_white_list = config.OTA_WHITE_LIST['abroad'].get(distributor)
            hks_white_list = config.OTA_WHITE_LIST['hks'].get(distributor)
            compared_list_tmp = copy.deepcopy(compared_list)
            ordered_list_tmp = copy.deepcopy(ordered_list)
            if area == 104:
                for supplier_id in compared_list_tmp:
                    if (int(supplier_id) in mainland_black_list) or (int(supplier_id) not in mainland_white_list):
                        compared_list.remove(str(supplier_id))
                for supplier_id in ordered_list_tmp:
                    if (int(supplier_id) in mainland_black_list) or (int(supplier_id) not in mainland_white_list):
                        ordered_list.remove(str(supplier_id))
            elif area in (107, 216):
                for supplier_id in compared_list_tmp:
                    if (int(supplier_id) in hks_black_list) or (int(supplier_id) not in hks_white_list):
                        compared_list.remove(str(supplier_id))
                for supplier_id in ordered_list_tmp:
                    if (int(supplier_id) in hks_black_list) or (int(supplier_id) not in hks_white_list):
                        ordered_list.remove(str(supplier_id))
            else:
                for supplier_id in compared_list_tmp:
                    if (int(supplier_id) in abroad_black_list) or (int(supplier_id) not in abroad_white_list):
                        compared_list.remove(str(supplier_id))
                for supplier_id in ordered_list_tmp:
                    if (int(supplier_id) in abroad_black_list) or (int(supplier_id) not in abroad_white_list):
                        ordered_list.remove(str(supplier_id))
            self.filter_specify_supplier(compared_list, ordered_list)
            if len(ordered_list) >= max_num:
                return ordered_list[:max_num]
            return self.merge(compared_list, ordered_list)[:max_num]
        except:
            self.logger.error(msg=traceback.format_exc())
            return self.merge(compared_list, ordered_list)[:max_num]

    def filter_specify_supplier(self, compared_list, ordered_list):
        """
        将一些敏感的供应商提高要求尽量过滤掉
        :param compared_list:
        :param ordered_list:
        :return:
        """
        try:
            for supplier_id, top in config.OTA_SUPPLIER_WEIGHT.items():
                if str(supplier_id) in compared_list[top:]:
                    compared_list.remove(str(supplier_id))
                if str(supplier_id) in ordered_list[top:]:
                    ordered_list.remove(str(supplier_id))
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def merge(self, compared_list, ordered_list):
        """
        将compared_list的供应商追加到ordered_list中, 去除重复项并保持顺序
        :param compared_list:
        :param ordered_list:
        :return:
        """
        try:
            ret = copy.deepcopy(ordered_list)
            for supplier_id in compared_list:
                if supplier_id in ret:
                    continue
                if int(supplier_id) in config.exchange_supplier:
                    if str(config.exchange_supplier[int(supplier_id)]) in ret:
                        continue
                # 比价选出的不再调度列表中的供应商忽略
                if self.scheduled_supplier:
                    if int(supplier_id) not in self.scheduled_supplier:
                        continue
                ret.append(supplier_id)
            return ret
        except:
            self.logger.error(msg=traceback.format_exc())
            return ordered_list + compared_list

    def get_hotel_order_suppliers(self, hq_hotel_id, days_before=30):
        """
        查看这个酒店最近一段时间产生过正常订单的供应商列表
        :param hq_hotel_id:
        :param days_before:
        :return: list
        """
        try:
            ordered_suppliers = list()
            day = datetime.date.today() - datetime.timedelta(days=days_before)
            sql = "SELECT distinct supplier_id, count(*) as c FROM hqs_order WHERE status=10 and order_id IN " \
                  "(SELECT order_id FROM hqs_order_hotel WHERE hotel_id=%s and create_time>='%s') " \
                  "GROUP BY supplier_id ORDER BY c DESC" % (hq_hotel_id, day)
            ret = pymysql_query(sql, Const.orders)
            if ret == DB_EXCEPTION:
                self.logger.error('Exec sql=%s error' % sql)
                return list()
            for row in ret:
                supplier_id = str(row[Const.supplier_id])
                ordered_suppliers.append(supplier_id)
            return ordered_suppliers
        except:
            self.logger.error(msg=traceback.format_exc())
            return list()

    def update_ctrip_hotel_supplier(self, ctrip_online_hotels, intl=False):
        """
        更新携程上线酒店优选供应商
        :return:
        """
        try:
            if len(ctrip_online_hotels.keys()) == 0:
                return True
            day = datetime.datetime.now() - datetime.timedelta(days=30 * 3)
            sql = "SELECT hq_hotel_id, substring_index(group_concat(supplier_id ORDER BY min_price_score DESC),',',10) AS 'top' " \
                  "FROM hotel_supplier_rank WHERE update_time > '%s' AND hq_hotel_id in (%s) GROUP BY hq_hotel_id" % \
                  (day, ','.join(ctrip_online_hotels.keys()))
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error('Exec sql=%s error' % sql)
                return False
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    ordered_suppliers = self.get_hotel_order_suppliers(hq_hotel_id)
                    # 排好序的供应商列表
                    top_list = row[Const.top].split(',')
                    if len(top_list) == 1 and top_list[0] == '0':
                        continue
                    area = Tools.get_hotel_area(hq_hotel_id)
                    if area == -1:
                        self.logger.error('get country error, hq_hotel_id=%s' % hq_hotel_id)
                        continue
                    # 如果酒店所在地区未在影响范围内,忽略 国家数据：104 中国大陆, 107 中国台湾, 216 中国港澳
                    if area in config.OTA_AREA_BLACK_LIST['ctrip']:
                        continue
                    ctrip_online_hotels[hq_hotel_id][Const.top] = top_list
                    ctrip_online_hotels[hq_hotel_id][Const.ordered_suppliers] = ordered_suppliers
                    # 将此酒店此端的黑名单供应商过滤掉,生成可上线的供应商
                    new_supplier_list = self.filter_blacklist('ctrip', area, top_list, ordered_suppliers,
                                                              ctrip_online_hotels[hq_hotel_id][
                                                                  Const.supplier_blacklist])
                    if config.CTRIP_BETA:
                        mainland_white_list = [str(i) for i in config.OTA_WHITE_LIST['mainland'].get('ctrip')]
                        abroad_white_list = [str(i) for i in config.OTA_WHITE_LIST['abroad'].get('ctrip')]
                        hks_white_list = [str(i) for i in config.OTA_WHITE_LIST['hks'].get('ctrip')]
                        if area == 104:
                            mergy = set(mainland_white_list) & set(
                                ctrip_online_hotels[hq_hotel_id][Const.currentsupplier])
                        elif area in (107, 216):
                            mergy = set(hks_white_list) & set(ctrip_online_hotels[hq_hotel_id][Const.currentsupplier])
                        else:
                            mergy = set(abroad_white_list) & set(
                                ctrip_online_hotels[hq_hotel_id][Const.currentsupplier])
                        new_supplier_list = list(set(new_supplier_list) | mergy)
                    ctrip_online_hotels[hq_hotel_id][Const.new_suppliers] = new_supplier_list
                    self.logger.info('@ctrip hq_hotel_id=%s info=%s' % (hq_hotel_id, ctrip_online_hotels[hq_hotel_id]))
                    gevent.sleep(0.01)
                except:
                    self.logger.error(msg=traceback.format_exc())
            # 更新携程上线表供应商列表
            cursor = start_transaction(Const.ota)
            for hq_hotel_id, item in ctrip_online_hotels.items():
                try:
                    old_list = set(item[Const.currentsupplier])
                    new_list = set(item[Const.new_suppliers])
                    if old_list == new_list:
                        continue
                    if intl:
                        sql = "UPDATE haoqiao_ctrip_intl_online_hotels SET supplier_id='%s', add_time=now() WHERE hotel_id=%s" % (
                            ','.join(new_list), hq_hotel_id)
                    else:
                        sql = "UPDATE haoqiao_ctrip_online_hotels SET supplier_id='%s', add_time=now() WHERE hotel_id=%s" % (
                            ','.join(new_list), hq_hotel_id)
                    pymysql_transaction_query(sql, cursor)
                except:
                    self.logger.error(msg=traceback.format_exc())
            commit_transaction(cursor)
            # 更新日志表
            self.update_supplier_change_logger_table('ctrip', ctrip_online_hotels)
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def update_qunar_hotel_supplier(self, qunar_online_hotels):
        """
        更新去哪上线酒店优选供应商
        :return:
        """
        try:
            if len(qunar_online_hotels.keys()) == 0:
                return True
            day = datetime.datetime.now() - datetime.timedelta(days=30 * 3)
            sql = "SELECT hq_hotel_id, substring_index(group_concat(supplier_id ORDER BY min_price_score DESC),',',10) AS 'top' " \
                  "FROM hotel_supplier_rank WHERE update_time > '%s' AND hq_hotel_id in (%s) GROUP BY hq_hotel_id" % \
                  (day, ','.join(qunar_online_hotels.keys()))
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error('Exec sql=%s error' % sql)
                return False
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    ordered_suppliers = self.get_hotel_order_suppliers(hq_hotel_id)
                    # 排好序的供应商列表
                    top_list = row[Const.top].split(',')
                    if len(top_list) == 1 and top_list[0] == '0':
                        continue
                    area = Tools.get_hotel_area(hq_hotel_id)
                    if area == -1:
                        self.logger.error('get country error, hq_hotel_id=%s' % hq_hotel_id)
                        continue
                    # 如果酒店所在地区未在影响范围内,忽略 国家数据：104 中国大陆, 107 中国台湾, 216 中国港澳
                    if area in config.OTA_AREA_BLACK_LIST['qunar']:
                        self.logger.info('@hq_hotel_id=%s area=%s ignore' % (hq_hotel_id, area))
                        continue
                    qunar_online_hotels[hq_hotel_id][Const.top] = top_list
                    qunar_online_hotels[hq_hotel_id][Const.ordered_suppliers] = ordered_suppliers
                    # 将此酒店此端的黑名单供应商过滤掉,生成可上线的供应商
                    new_supplier_list = self.filter_blacklist('qunar', area, top_list, ordered_suppliers,
                                                              qunar_online_hotels[hq_hotel_id][
                                                                  Const.supplier_blacklist])
                    qunar_online_hotels[hq_hotel_id][Const.new_suppliers] = new_supplier_list
                    self.logger.info('@qunar hq_hotel_id=%s info=%s' % (hq_hotel_id, qunar_online_hotels[hq_hotel_id]))
                    gevent.sleep(0.01)
                except:
                    self.logger.error(msg=traceback.format_exc())
            cursor = start_transaction(Const.ota)
            for hq_hotel_id, item in qunar_online_hotels.items():
                try:
                    old_list = set(item[Const.currentsupplier])
                    new_list = set(item[Const.new_suppliers])
                    if old_list == new_list:
                        continue
                    sql = "UPDATE haoqiao_qunar_online_hotels SET supplier_id='%s', add_time=now() WHERE hotel_id=%s" % (
                        ','.join(new_list), hq_hotel_id)
                    pymysql_transaction_query(sql, cursor)
                except:
                    self.logger.error(msg=traceback.format_exc())
            commit_transaction(cursor)
            # 更新日志表
            self.update_supplier_change_logger_table('qunar', qunar_online_hotels)
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def update_qunar_spec_hotel_supplier(self, qunar_spec_online_hotels):
        """
        更新去哪上线酒店优选供应商, 为马甲配置二等,三等供应商到haoqiao_ota_qunar_hotels
        :return:
        """
        try:
            # 去哪马甲id
            qunar_account_id_list = [3, 4]
            if len(qunar_spec_online_hotels.keys()) == 0:
                return True
            sql = "SELECT hq_hotel_id, substring_index(group_concat(supplier_id ORDER BY min_price_score DESC),',',30) AS 'top' " \
                  "FROM hotel_supplier_rank WHERE hq_hotel_id in (%s) GROUP BY hq_hotel_id" % ','.join(
                qunar_spec_online_hotels.keys())
            ret = pymysql_query(sql, Const.schedule)
            if ret == DB_EXCEPTION:
                self.logger.error('Exec sql=%s error' % sql)
                return False
            cursor = start_transaction(Const.ota)
            for row in ret:
                try:
                    hq_hotel_id = str(row[Const.hq_hotel_id])
                    ordered_suppliers = self.get_hotel_order_suppliers(hq_hotel_id)
                    # 排好序的供应商列表
                    top_list = row[Const.top].split(',')
                    if len(top_list) == 1 and top_list[0] == '0':
                        continue
                    area = Tools.get_hotel_area(hq_hotel_id)
                    if area == -1:
                        self.logger.error('get country error, hq_hotel_id=%s' % hq_hotel_id)
                        continue
                    # 如果酒店所在地区未在影响范围内,忽略 国家数据：104 中国大陆, 107 中国台湾, 216 中国港澳
                    if area in config.OTA_AREA_BLACK_LIST['qunar']:
                        self.logger.info('@hq_hotel_id=%s area=%s ignore' % (hq_hotel_id, area))
                        continue
                    qunar_spec_online_hotels[hq_hotel_id][Const.top] = top_list
                    qunar_spec_online_hotels[hq_hotel_id][Const.ordered_suppliers] = ordered_suppliers
                    # 将此酒店此端的黑名单供应商过滤掉,生成可上线的供应商
                    first_group = self.filter_blacklist('qunar', area, top_list, ordered_suppliers,
                                                        qunar_spec_online_hotels[hq_hotel_id][Const.supplier_blacklist])
                    mainland_white_list = [str(i) for i in config.OTA_WHITE_LIST['mainland'].get('qunar')]
                    abroad_white_list = [str(i) for i in config.OTA_WHITE_LIST['abroad'].get('qunar')]
                    hks_white_list = [str(i) for i in config.OTA_WHITE_LIST['hks'].get('qunar')]
                    if area == 104:
                        supplier_list = list(set(mainland_white_list) & set(top_list) - set(first_group) - set(
                            config.OTA_SUPPLIER_WEIGHT.keys()))
                    elif area in (107, 216):
                        supplier_list = list(set(hks_white_list) & set(top_list) - set(first_group) - set(
                            config.OTA_SUPPLIER_WEIGHT.keys()))
                    else:
                        supplier_list = list(set(abroad_white_list) & set(top_list) - set(first_group) - set(
                            config.OTA_SUPPLIER_WEIGHT.keys()))
                    x = 0
                    for qunar_account_id in qunar_account_id_list:
                        if len(supplier_list) < 5:
                            new_list = supplier_list
                        else:
                            new_list = supplier_list[x:x + 5]
                        x += 5
                        sql = "UPDATE haoqiao_ota_qunar_hotels SET supplier_id='%s', add_time=now() WHERE hotel_id=%s and qunar_account_id=%s" % (
                            ','.join(new_list), hq_hotel_id, qunar_account_id)
                        pymysql_transaction_query(sql, cursor)
                    self.logger.info(
                        '@qunar hq_hotel_id=%s info=%s' % (hq_hotel_id, qunar_spec_online_hotels[hq_hotel_id]))
                    gevent.sleep(0.01)
                except:
                    self.logger.error(msg=traceback.format_exc())
            commit_transaction(cursor)
            return True
        except:
            self.logger.error(msg=traceback.format_exc())
            return False

    def pick_hq_hotels2(self):
        """
        选取参与对比的好巧酒店集合
        :return:
        """
        try:
            picker = Picker(self.logger)
            return picker.pick()
        except:
            self.logger.error(msg=traceback.format_exc())
            return set()

    def pick_hq_hotels(self):
        """
        使用配置表形式获取任务
        :return:
        """
        try:
            picker = Picker(self.logger)
            return picker.pick_from_task_table()
        except:
            self.logger.error(msg=traceback.format_exc())
            return set()

    def sync_task_worker(self):
        """
        将从接口收集到的任务,同步到数据库任务表中
        :return:
        """
        key = config.COMPARE_TASK_KEY
        while True:
            try:
                ret = self.redis_handler.SMEMBERS(key)
                if ret:
                    self.update_compare_task(ret)
                gevent.sleep(10 * 60)
            except:
                self.logger.error(msg=traceback.format_exc())

    def update_compare_task(self, task_set):
        """
        将task_set中的酒店任务写入到比价任务表中
        :param task_set:
        :return: bool
        """
        try:
            sql_format = "INSERT IGNORE INTO compare_hotel_task(hq_hotel_id,status,create_time) VALUES %s"
            values = list()
            now = datetime.datetime.now()
            for hq_hotel_id in task_set:
                value = "(%s,1,'%s')" % (hq_hotel_id, now)
                values.append(value)
            sql = sql_format % ','.join(values)
            cursor = start_transaction(Const.schedule)
            pymysql_transaction_query(sql, cursor)
            commit_transaction(cursor)
        except:
            self.logger.error(msg=traceback.format_exc())
            return False


def handle_sear_server_request(str_req_url, req_params_map, str_req_path):
    """
    向搜索服务发送请求
    :param strReqUrl:
    :param reqParamsMap:
    :param strReqPath:
    :return:
    """
    str_rsp_content = '';
    try:

        str_url = 'http://%s:%s%s?channel=poi' %(config.SEARCH_SERVER_IP,config.SEARCH_SERVER_PORT,str_req_path)
        if not str_req_path:
            str_url+='&'+str_req_url

    except:
        pass

def handle_compare_request(request_dict, session_id):
    """
    接收poi检索请求并处理
    :param request_dict:
    :param session_id:
    :return:
    """
    try:
        if not isinstance(request_dict, dict):
            return {'code': -1, 'msg': 'param error', 'data': [], 'session_id': session_id}
        hq_hotel_id_set = set(request_dict.get('hotels', '').split(','))
        if not hq_hotel_id_set:
            return {'code': -1, 'msg': 'param error', 'data': [], 'session_id': session_id}
        froms = request_dict.get('from', '')
        if not froms:
            return {'code': -1, 'msg': 'param error', 'data': [], 'session_id': session_id}
        task_type = request_dict.get('type', '')
        if not task_type:
            return {'code': -1, 'msg': 'param error', 'data': [], 'session_id': session_id}
        # 将收到的任务写入到codis的任务集合中
        send_task_to_table(hq_hotel_id_set, froms, task_type)
        # worker = Compare(hq_hotel_id_set, AliLog)
        # gevent.spawn(worker.work)
        return {'code': 0, 'msg': 'ok', 'data': [], 'session_id': session_id}
    except:
        AliLog.error(traceback.format_exc())
        return {'code': -1, 'msg': 'param error', 'data': [], 'session_id': session_id}


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)


# 初始化服务
def init_app(app):
    try:
        # 设置utf-8环境
        reload(sys)
        sys.setdefaultencoding('utf-8')

        # 初始化本地日志模块
        cur_path = os.path.dirname(os.path.abspath(__file__))
        log_path = os.path.join(cur_path, 'log')
        if not Logger.init(log_path, log_level=config.APP_LOG_LEVEL, log_name='compare.log'):
            sys.exit(1)

        # 初始化阿里日志服务
        if not AliLog.init(to_ali_log=config.APP_LOG_TO_ALIYUN, log_level=config.APP_LOG_LEVEL,
                           log_project=config.APP_LOG_PROJECT, log_store=config.APP_LOG_STORE,
                           topic=config.APP_LOG_TOPIC, endpoint=config.END_POINT,
                           access_key_id=config.ACCESS_KEY_ID, access_key=config.ACCESS_KEY):
            sys.exit(1)

        # 初始化Tools
        Tools.set_logger(AliLog)

        # 设置gunicorn日志级别
        app.logger.setLevel(config.APP_LOG_LEVEL)
        app.logger.addHandler(Logger.logger)

        # 设置requests日志级别
        logging.getLogger("requests").setLevel(logging.WARNING)

        # 初始化mysql
        # cur_dir = os.path.dirname(os.path.abspath(__file__))
        # ini_dir = os.path.join(cur_dir, 'public')
        if not pymysql_init(logger=AliLog, hosts=config.mysql_ip_list):
            AliLog.error(msg="mysql_init failed")
            return False

        # 启动比价处理模块
        handler = CompareServer(AliLog, redis_handler)
        gevent.spawn(handler.run)
        return True
    except:
        # 启动失败退出进程
        msg = traceback.format_exc()
        print (msg)
        Logger.logger.error(msg)
        sys.exit(1)


# 初始化codis集群
redis_handler = redis.Redis(config.CODIS_SERVER_HOST, config.CODIS_SERVER_PORT)

# 初始化应用程序
init_app(app)


@app.route('/s')
def poi_search():
    session_id = Tools.create_session_id()
    try:
        # 解析数据
        request_dict = request.args.to_dict()
        # 处理请求
        result_dict = handle_compare_request(request_dict, session_id)
        # 返回操作结果
        result_string = json.dumps(result_dict)
        response = make_response(result_string)
        return response
    except:
        AliLog.error('update_hotels except[%s]' % traceback.format_exc(), session_id)
        return 'error'


if __name__ == "__main__":
    app.run()

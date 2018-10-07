#!/usr/bin/env python
# -*- coding=utf-8 -*-

from gevent import monkey; monkey.patch_all()
import gevent
import sys
sys.path.append('../')
import datetime
import config
import requests
from public.tools import Tools
from public.consts import Const
from public.pymysql_adapter import *
from public.mapping import Mapping


class Compare(object):
    """

    """
    def __init__(self, hq_hotel_id_set, logger, ota_hotels=None):
        self.hq_hotel_id_set = hq_hotel_id_set
        self.logger = logger
        self.download_suppliers = Tools.get_dls_suppliers()
        self.ota_hotels = ota_hotels
        self.hotel_city_map = dict()
        """记录ota各个端的酒店信息"""

    def work(self):
        """
        将分配下来的所有好巧酒店进行供应商优选, 下发所有供应商酒店, 对比房型价格排序
        :return:
        """
        try:
            workers = list()
            tmp_hotel_set = set()
            for hq_hotel_id in self.hq_hotel_id_set:
                workers.append(gevent.spawn(self.compare_hotel, hq_hotel_id))
                tmp_hotel_set.add(hq_hotel_id)
                # 一千条更新一次酒店状态
                if len(tmp_hotel_set) >= 500:
                    self.update_task_status(tmp_hotel_set)
                    tmp_hotel_set = set()
                gevent.sleep(10)
            if len(tmp_hotel_set) > 0:
                self.update_task_status(tmp_hotel_set)
            for t in workers:
                t.join()
            print '[%s] compared finish' % self.hq_hotel_id_set
            return
        except:
            self.logger.error(traceback.format_exc())
            return

    def update_task_status(self, compared_hotel):
        """
        将已经比价过的酒店,在任务表中的状态进行更新
        :param compared_hotel
        :return:
        """
        try:
            sql_format = "UPDATE compare_hotel_task SET update_time=NOW(),status=(CASE WHEN task_type!=2 THEN 1 ELSE 0 END ) WHERE %s"
            values = list()
            for hq_hotel_id in compared_hotel:
                # 1:单次任务, 2:周期性任务, 3:紧急任务
                value = "(hq_hotel_id=%s)" % hq_hotel_id
                values.append(value)
            sql = sql_format % ' or '.join(values)
            print sql
            cursor = start_transaction(Const.schedule)
            pymysql_transaction_query(sql, cursor)
            commit_transaction(cursor)
            return True
        except:
            self.logger.error(traceback.format_exc())
            return False

    def compare_hotel(self, hq_hotel_id):
        """
        比价此酒店下所有供应商的酒店, 包括对所有供应商酒店日期的价格下发, 规整, 排序, 统计
        :param hq_hotel_id:
        :return:
        """
        try:
            workers = list()
            ret_info = dict()
            # 获取所有供应商酒店信息
            hotel_sp_info = Mapping.get_all_hotel_info(hq_hotel_id, self.logger)
            """((supplier_id, hq_hotel_id ,sp_hotel_code, sp_city_code, sp_city_name, hq_city_id),(...))"""
            if not hotel_sp_info:
                self.logger.error('hq_hotel_id=%s get Nothing diffuse_supplier_info' % hq_hotel_id)
                return
            # 挑选带下发的如理日期对
            days = self.pick_days_pair(hq_hotel_id)
            """[(checkin1,checkout1),(checkin2,checkout2)]"""
            # 下发任务
            for params in hotel_sp_info:
                try:
                    workers.append(gevent.spawn(self.collection_price, params, days, ret_info))
                except:
                    self.logger.error(traceback.format_exc())
            # 等待全部请求返回
            for j in workers:
                j.join()
            # 处理排名
            score_info = self.compare_price(hq_hotel_id, ret_info)
            """dict{'regular_rooms': [supplier_id]:scores, 'min_price':[supplier_id]:scores}"""
            if not score_info:
                return
            # 记录此酒店各供应商情况
            self.record_hotel_rank_to_db(hq_hotel_id, score_info)
            return
        except:
            self.logger.error(traceback.format_exc())
            return

    def record_hotel_rank_to_db(self, hq_hotel_id, score_info):
        """
        记录此酒店各个供应商的排名情况, 记录到schedule库hotel_supplier_rank表中
        :param hq_hotel_id:
        :param score_info: dict{'regular_rooms': [supplier_id]:scores, 'min_price':[supplier_id]:scores}
        :return: bool
        """
        try:
            values = list()
            now = datetime.datetime.now()
            hq_city_id = self.hotel_city_map.get(int(hq_hotel_id), None)
            for supplier_id, regular_score in score_info[Const.regular_rooms].items():
                try:
                    min_price_score = score_info[Const.min_price].get(supplier_id, 0)
                    value = "(%s,%s,%s,%s,%s,'%s')" % (hq_hotel_id, supplier_id, hq_city_id, regular_score, min_price_score, now)
                    values.append(value)
                except:
                    self.logger.error(traceback.format_exc())
            if values:
                sql = "REPLACE INTO hotel_supplier_rank(hq_hotel_id, supplier_id, hq_city_id, regular_rooms_score, min_price_score, update_time) VALUES %s" % ','.join(values)
            else:
                sql = "REPLACE INTO hotel_supplier_rank(hq_hotel_id, supplier_id, hq_city_id, regular_rooms_score, min_price_score, update_time) VALUES (%s,0,%s, 0, 0,'%s')" % (hq_hotel_id, hq_city_id, now)
            cursor = start_transaction(Const.schedule)
            pymysql_transaction_query(sql, cursor)
            commit_transaction(cursor)
            return
        except:
            self.logger.error(traceback.format_exc())
            return

    def compare_price(self, hq_hotel_id, ret_info):
        """
        对比此酒店价格请求结果, 需要统计出同一天同一房型 所有供应商排名前10位
        比如房型 A 所有供应商价格排序, B所有供应商价格排序...
        :param hq_hotel_id:
        :param ret_info:
        :return: 对比后的结果 每个供应商获得的总分
        """
        try:
            # 统计出所有供应商每种房型的总体得分
            all_regular_score = dict()
            # 统计出最低价供应商得分
            min_supplier_score = dict()
            # 统计出最低价的订单
            for (checkin, checkout), item in ret_info.items():
                try:
                    all_regular_info = dict()
                    """所有房型比价后数据all_regular_info[房型名][供应商名]:最低价"""
                    min_price_info = dict()
                    """只比最低价 min_price_info [供应商名]:最低价"""
                    # 此日期下所有供应商套餐规整, 找出供应商每个房型的最低价
                    for supplier_id, rate_sets in item.items():
                        if supplier_id not in all_regular_score:
                            all_regular_score[supplier_id] = 0
                        # 规整供应商房型价格排序
                        if not self.count_supplier_min_room_name_price(supplier_id, rate_sets, all_regular_info, min_price_info):
                            continue
                    msg = 'hq_hotel_id=%s days=(%s-%s) 相关房型下各供应商最低价格概况:[%s]' % (hq_hotel_id, checkin, checkout, all_regular_info)
                    self.logger.info(msg)
                    # 算好此日期下所有供应商所有套餐的得分
                    for regular_room_name, tmp_item in all_regular_info.items():
                        sorted_supplier_price_info = sorted(tmp_item.items(), lambda x, y: cmp(x[1], y[1]), reverse=False)
                        self.count_supplier_score(sorted_supplier_price_info, all_regular_score)
                    # 计算供应商最低价得分
                    sorted_min_supplier_price_info = sorted(min_price_info.items(), lambda x, y: cmp(x[1], y[1]), reverse=False)
                    self.count_supplier_score(sorted_min_supplier_price_info, min_supplier_score)
                except:
                    self.logger.error(traceback.format_exc())
            result = {
                Const.regular_rooms: all_regular_score,
                Const.min_price: min_supplier_score
            }
            return result
        except:
            self.logger.error(traceback.format_exc())
            return None

    def count_supplier_score(self, sorted_supplier_price_info, score_info):
        """
        将已经排序过的房型:供应商:价格列表, 根据规则为每个供应商计算得分, 结果写到rank中
        目前按照排名第一的得10分, 第二的得9分依次类推, 排名10以后的均不计分
        :param sorted_supplier_price_info: [(supplier_id, price),(supplier_id, price)...]
        :param score_info: dict rank[supplier_id] = int
        :return:
        """
        try:
            i = 20
            for (supplier_id, price) in sorted_supplier_price_info:
                if supplier_id not in score_info:
                    score_info[supplier_id] = 0
                score_info[supplier_id] += i
                i -= 1
                if i <= 0:
                    break
            return True
        except:
            self.logger.error(traceback.format_exc())
            return False

    def count_supplier_min_room_name_price(self, supplier_id, rate_sets, regular_info, min_price_info):
        """
        计算出此供应商荣有的房型中的所有房型的最低价, 结果写到regular_info中,格式为,房型下:供应商:价格
        :param supplier_id:
        :param rate_sets:
        :param regular_info: regular_info[房型名][供应商名]:最低价
        :param min_price_info 所有房型中的最低价供应商信息
        min_price_info[supplier_id]:最低价
        :return: bool
        """
        try:
            tmp_supplier_room_min_price = dict()
            # 规整所有房型的最低价
            for rate in rate_sets:
                try:
                    if not rate:
                        continue
                    regular_room_name = rate[Const.regular_room_name]
                    price = float(rate[Const.price_sale])
                    if regular_room_name not in tmp_supplier_room_min_price:
                        tmp_supplier_room_min_price[regular_room_name] = price
                    # 只存此房型里价格最低的
                    if tmp_supplier_room_min_price[regular_room_name] != 0 and price < tmp_supplier_room_min_price[regular_room_name]:
                        tmp_supplier_room_min_price[regular_room_name] = price
                    # 统计此供应商酒店最低价
                    if not min_price_info:
                        min_price_info[supplier_id] = price
                    else:
                        if supplier_id not in min_price_info:
                            min_price_info[supplier_id] = price
                        if price < min_price_info[supplier_id]:
                            min_price_info[supplier_id] = price
                except:
                    self.logger.error(traceback.format_exc())
            # 将结果写到regular_info中
            if not tmp_supplier_room_min_price:
                return
            for regular_room_name, price in tmp_supplier_room_min_price.items():
                if regular_room_name not in regular_info:
                    regular_info[regular_room_name] = dict()
                # 此房型此供应商的最低价
                regular_info[regular_room_name][supplier_id] = price
            return True
        except:
            self.logger.error(traceback.format_exc())
            return False

    def collection_price(self, params, days, ret_info):
        """
        针对参数日期, 获取价格数据
        :param params:
        (supplier_id, hq_hotel_id ,sp_hotel_code, sp_city_code, sp_city_name, hq_city_id)
        :param days:
        [(checkin1,checkout1),(checkin2,checkout2)]
        :param ret_info: 存放结果的, 每天下所有供应商的酒店价格信息 dict
        :return:
        """
        try:
            (supplier_id, hq_hotel_id, sp_hotel_code, sp_city_code, sp_city_name, hq_city_id) = params
            if int(hq_hotel_id) not in self.hotel_city_map:
                self.hotel_city_map[int(hq_hotel_id)] = hq_city_id
            if int(supplier_id) in config.SUPPLIER_BLACK_LIST:
                return
            for (checkin, checkout) in days:
                try:
                    if int(supplier_id) not in self.download_suppliers:
                        continue
                    sequence = 'Compare_%s' % Tools.create_session_id()
                    cache_url = Tools.make_cache_url(supplier_id, hq_hotel_id, sp_hotel_code, hq_city_id, checkin, checkout,
                                                     sp_city_code, sp_city_name, sequence)
                    price_data = self.get_cache_price_data(cache_url)
                    if price_data:
                        self.collection_price_to_dict(supplier_id, price_data, checkin, checkout, ret_info)
                        continue
                    dls_url = Tools.make_dls_url(supplier_id, hq_hotel_id, sp_hotel_code, hq_city_id, checkin, checkout,
                                                 sp_city_code, sp_city_name, sequence)
                    price_data = self.get_dls_price_data(dls_url)
                    if price_data:
                        self.collection_price_to_dict(supplier_id, price_data, checkin, checkout, ret_info)
                    else:   # 无房情况
                        pass
                except:
                    self.logger.error(traceback.format_exc())
            return
        except:
            self.logger.error(traceback.format_exc())
            return

    def collection_price_to_dict(self, supplier_id, price_data, checkin, checkout, ret_info):
        """
        将拿到的套餐数据收集到ret_info中
        :param supplier_id:
        :param price_data:
        :param checkin:
        :param checkout:
        :param ret_info:
        :return: bool
        """
        try:
            if (checkin, checkout) not in ret_info:
                ret_info[(checkin, checkout)] = dict()
            if supplier_id not in ret_info[(checkin, checkout)]:
                ret_info[(checkin, checkout)][supplier_id] = list()
            for rate in price_data:
                ret_info[(checkin, checkout)][supplier_id].append(rate)
            return True
        except:
            self.logger.error(traceback.format_exc())
            return False

    def get_dls_price_data(self, dls_url):
        """
        获取url对应的下载酒店价格数据
        :param dls_url:
        :return: data
        """
        try:
            ret = requests.get(dls_url, 65)
            if not ret:
                return None
            ret = ret.json()
            return ret[Const.data]
        except:
            self.logger.error('url=[%s] except:%s' % (dls_url, traceback.format_exc()))
            return None

    def get_cache_price_data(self, cache_url):
        """
        获取url对应的缓存酒店价格数据
        :param cache_url:
        :return: data
        """
        try:
            ret = requests.get(cache_url, 3)
            if not ret:
                return None
            ret = ret.json()
            for key in ret[Const.data]:
                return ret[Const.data][key][Const.data]
            return None
        except:
            self.logger.error('url=[%s] except:%s' % (cache_url, traceback.format_exc()))
            return None

    def pick_days_pair(self, hq_hotel_id, num=10):
        """
        为酒店挑选入住日期
        :param hq_hotel_id:
        :param num: 吐出数量
        :return:
        """
        try:
            today = datetime.date.today()
            days = set()
            for i in range(1, num+1):
                offset = 5 * i
                check_in = today + datetime.timedelta(days=offset)
                check_out = check_in + datetime.timedelta(days=1)
                days.add((str(check_in), str(check_out)))
            return days
        except:
            self.logger.error(traceback.format_exc())
            return None


# 初始化服务
def init_app():
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

        # 设置requests日志级别
        logging.getLogger("requests").setLevel(logging.WARNING)

        # 初始化mysql
        if not pymysql_init(logger=AliLog, hosts=config.mysql_ip_list):
            AliLog.error(msg="mysql_init failed")
            return False

        return True
    except:
        # 启动失败退出进程
        msg = traceback.format_exc()
        print msg
        sys.exit(1)

if __name__ == '__main__':
    from public.logger import Logger
    from public.logger import AliLog
    from test import *
    # 初始化应用程序
    init_app()
    hq_hotel_set = get_test_hotels()
    worker = Compare(hq_hotel_set, AliLog)
    worker.work()
    # 校验结果
    check_result(hq_hotel_set)

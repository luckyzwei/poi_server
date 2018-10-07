#!/usr/bin/env python
# -*- coding=utf-8 -*-
"""
    负责将传入的酒店数据进行发散调度
"""

from mapping import Mapping
import datetime
import traceback


class Diffuser(object):
    def __init__(self):
        pass

    @classmethod
    def diffuse(cls, pair_sets, logger=None):
        """
        将pair_sets 中的好巧酒店信息
        :param pair_sets:
        :return:
        """
        try:
            fat_pair_list = list()
            for pair in pair_sets:
                try:
                    (hq_hotel_id, sp_hotel_id, supplier_id, checkin, checkout) = pair
                    check_pair_list = cls.diffuse_check_day(checkin, checkout)
                    # 发散酒店
                    supplier_hotel_list = cls.diffuse_supplier_info(hq_hotel_id=hq_hotel_id, max_diffuse_num=5, need_supplier=0, logger=logger)
                    """((supplier_id, hq_hotel_id ,sp_hotel_code, sp_city_code, sp_city_name, hq_city_id), (...))"""
                    for supplier_hotel in supplier_hotel_list:
                        # 发散日期
                        (supplier_id, hq_hotel_id, sp_hotel_code, sp_city_code, sp_city_name, hq_city_id) = supplier_hotel
                        for (checkin_i, checkout_i) in check_pair_list:
                            item = (supplier_id, hq_hotel_id, sp_hotel_code, sp_city_code, sp_city_name, hq_city_id, checkin_i, checkout_i)
                            print item
                            fat_pair_list.append(item)
                except:
                    logger.error(traceback.format_exc())
            logger.info('from %d expand to %d' % (len(pair_sets), len(fat_pair_list)))
            return fat_pair_list
        except:
            logger.error(traceback.format_exc())

    @classmethod
    def diffuse_supplier_info(cls, hq_hotel_id, max_diffuse_num=0, need_supplier=0, logger=None):
        """
        根据好巧酒店id,调用优选供应商服务,最多max_diffuse_num个酒店供应商信息
        :param hq_hotel_id:
        :param need_supplier: 指定需要返回的 0为无指定
        :param max_diffuse_num: 最大扩展个数 0 为全部
        :return: supplier_info_list
        """
        try:
            good_supplier_hotel_list = Mapping.get_good_supplier(hq_hotel_id, max_diffuse_num, need_supplier, logger)
            """((supplier_id, hq_hotel_id ,sp_hotel_code, sp_city_code, sp_city_name, hq_city_id),(...))"""
            ret_list = list()
            if good_supplier_hotel_list:
                for pair in good_supplier_hotel_list:
                    ret_list.append(pair)
            return ret_list
        except:
            logger.error(traceback.format_exc())
            return list()

    @classmethod
    def diffuse_supplier_set(cls, hq_hotel_id, max_diffuse_num=5, need_supplier=0, logger=None):
        """
        根据好巧酒店id,调用优选供应商服务,最多max_diffuse_num个供应商列表
        :param hq_hotel_id:
        :param max_diffuse_num:
        :return: supplier_set
        """
        try:
            good_supplier_hotel_list = Mapping.get_good_supplier(hq_hotel_id, max_diffuse_num, need_supplier, logger)
            """(supplier_id, hq_hotel_id ,sp_hotel_code, sp_city_code, sp_city_name, hq_city_id),(...))"""
            supplier_set = set()
            if good_supplier_hotel_list:
                for pair in good_supplier_hotel_list:
                    (supplier_id, hq_hotel_id, sp_hotel_code, sp_city_code, sp_city_name, hq_city_id) = pair
                    supplier_set.add(supplier_id)
            return supplier_set
        except:
            logger.error(traceback.format_exc())
            return set()

    @classmethod
    def diffuse_check_day(cls, check_in, check_out, expand=2, logger=None):
        """
        将传入的日期进行单天切断后在扩展,  前后扩展expand 天
        :param check_in:
        :type check_in date
        :param check_out:
        :type check_out date
        :param expand:
        :type expand int
        :return: list((checkin,checkout),..)
        """
        try:
            checkin_tmp = max(datetime.date.today(), check_in-datetime.timedelta(days=expand))
            checkout_tmp = check_out+datetime.timedelta(days=expand)
            if checkout_tmp < datetime.date.today():
                return list()
            return split_days(str(checkin_tmp), str(checkout_tmp), split_day=1, split=1)
        except:
            logger.error(traceback.format_exc())
            return list()


def split_days(check_in, check_out, split_day=1, split=0):
    """
    :type check_in: str
    :type check_out: str
    :type split_day: int
    :type split: int
    :return: list[tuple[str, str]]
    """
    try:
        date_tuple_list = list()

        check_in_date = datetime.datetime.strptime(check_in, '%Y-%m-%d')
        check_out_date = datetime.datetime.strptime(check_out, '%Y-%m-%d')
        date_delta = datetime.timedelta(days=split_day)

        # 不需要分片或不能分片
        if split == 0 or split_day <= 0:
            date_tuple = (check_in_date.strftime('%Y-%m-%d'), check_out_date.strftime('%Y-%m-%d'))
            date_tuple_list.append(date_tuple)
            return date_tuple_list

        b_date = check_in_date
        e_date = check_out_date
        while True:
            # 尝试找到节点
            m_date = b_date + date_delta
            if m_date >= e_date:
                break
            # 加入节点
            date_tuple = (b_date.strftime('%Y-%m-%d'), m_date.strftime('%Y-%m-%d'))
            date_tuple_list.append(date_tuple)
            # 准备下一次循环
            b_date = m_date
        # 加入尾巴
        date_tuple = (b_date.strftime('%Y-%m-%d'), e_date.strftime('%Y-%m-%d'))
        date_tuple_list.append(date_tuple)
        return date_tuple_list
    except:
        # cls.__error(traceback.format_exc())
        return list()

if __name__ == '__main__':
    pass


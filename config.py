#!/usr/bin/env python
# -*- coding=utf-8 -*-

import os
import sys
import multiprocessing
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import account

__cur_path = os.path.dirname(os.path.abspath(__file__))
__bin_path = os.path.join(__cur_path, "bin")
__log_path = os.path.join(__cur_path, "log")

# *******************************************************
# 注：部署新程序，请修改:
# bind(进程监听端口)
# proc_name(进程名)
# APP_REDIS_HOST(redis地址)
# APP_REDIS_PORT(redis端口)
# APP_LOG_TO_ALIYUN(是否记录阿里日志)
# APP_LOG_LEVEL(日志级别)
# *******************************************************

# ********************************************************
# 1.以下配置为gunicorn配置，名字不能随便改
# ********************************************************
bind = "0.0.0.0:33355"
daemon = True
proc_name = "pio_server"
worker_connections = 10000
pidfile = os.path.join(__bin_path, "poi_server.pid")
errorlog = os.path.join(__log_path, "gunicorn.log")
workers = 1
worker_class = "gevent"
loglevel = "error"
debug = False
timeout = 900
x_forwarded_for_header = 'X-FORWARDED-FOR'

# *********************************************************
# 3.以下配置为配置，名字可自己可以控制
# *********************************************************

# ************************ 搜索服务配置 ****************************
SEARCH_SERVER_IP='internal-search-server.haoqiao.com'
SEARCH_SERVER_PORT='9001'
#允许最大连接数
MAX_CONNECTIONS_NUM=0
# ************************ 日志 ****************************
# 阿里日志
APP_LOG_PROJECT = 'test'
# 阿里日志
APP_LOG_STORE = 'poi-test'
# 阿里topic
APP_LOG_TOPIC = 'poi_server'
# 阿里登陆信息
END_POINT = account.ALI_END_POINT
ACCESS_KEY_ID = account.ALI_ACCESS_KEY_ID
ACCESS_KEY = account.ALI_ACCESS_KEY
# 是否记到阿里日志
APP_LOG_TO_ALIYUN = False
# 日志级别
APP_LOG_LEVEL = logging.INFO

# 数据库ip集
mysql_ip_list = account.mysql_ip_list

# 是否选取订单酒店
ORDER_FLAG = False

# 环境设置
BASE_ENV = 'online'
if BASE_ENV == 'online':
    # 下载服务url
    DLS_URL_HEADER = 'http://hqs_download.haoqiao.cn:33020/'
    # 缓存服务url
    CACHE_SERVER_HEADER = 'http://hqs_cache.haoqiao.cn:33010/'
    # hqs price接口地址
    HQS_PRICE_URL_HEADER = 'http://online.hqs.haoqiao.cn/base/price?'
    # hqs price_list接口地址
    HQS_PRICELIST_URL_HEADER = 'http://online.hqs.haoqiao.cn/base/price_list?'
    # 映射服务
    ROOM_MAPPING_IP = "http://mapping.room.haoqiao.cn"
    ROOM_MAPPING_PORT = 30001
    ROOM_MAPPING_TIMEOUT = 10
    # codis
    CODIS_SERVER_HOST = account.CACHE_CODIS_SERVER_HOST
    CODIS_SERVER_PORT = account.CACHE_CODIS_SERVER_PORT
elif BASE_ENV == 'sandbox':
    # 下载服务url
    DLS_URL_HEADER = 'http://sandbox.haoqiao.com:33020/'
    # 缓存服务url
    CACHE_SERVER_HEADER = 'http://sandbox.haoqiao.com:33010/'
    # hqs price接口地址
    HQS_PRICELIST_URL_HEADER = 'http://sandbox.hqs.haoqiao.cn/base/price_list?'
    # 映射服务
    ROOM_MAPPING_IP = "http://mapping.room.haoqiao.cn"
    ROOM_MAPPING_PORT = 30001
    ROOM_MAPPING_TIMEOUT = 10
    # codis
    CODIS_SERVER_HOST = account.CODIS_SERVER_HOST_37
    CODIS_SERVER_PORT = account.CODIS_SERVER_PORT_37

else:   # 沙盒 环境
    # 下载服务url
    DLS_URL_HEADER = 'http://sandbox.haoqiao.com:33020/'
    # 缓存服务url
    CACHE_SERVER_HEADER = 'http://sandbox.haoqiao.com:33010/'
    # hqs price接口地址
    HQS_PRICE_URL_HEADER = 'http://sandbox.hqs.haoqiao.cn/base/price?'
    # 下载服务接口地址
    HQS_QUOTED_PRICE_URL_HEADER = 'http://hqs_download.haoqiao.cn?'
    # 映射服务
    ROOM_MAPPING_IP = "http://mapping.room.haoqiao.cn"
    ROOM_MAPPING_PORT = 30001
    ROOM_MAPPING_TIMEOUT = 10
    # codis
    CODIS_SERVER_HOST = account.CODIS_SERVER_HOST_78
    CODIS_SERVER_PORT = account.CODIS_SERVER_PORT_78

COMPARE_TASK_KEY = 'hqs:schedule:compare:hotel:task:set'

COMPARE_EFFECT_DAYS = 30
# mapping 服务地址,获取供应商酒店信息
MAPPING_SERVER_URL = 'http://mask-control.haoqiao.com:50000/s2h?type=hotellist&supplier=%s&hq_city_id=0&page=%s&pagesize=10000&service_channel=6'     # format supplier_id,hq_city_id,page

# 调度服务地址
COMMON_SCHEDULE_BASE_URL = 'http://schedule.haoqiao.com:33300'

# 调度服务调度任务上线标记
ONLINE = 0
OFFLINE = 1

# 供应商黑名单
SUPPLIER_BLACK_LIST = [41]

# 任务类型 (1:单次任务, 2:周期性任务, 3:紧急任务)
TYPE_ONE = 1
TYPE_CYCLE = 2
TYPE_EMERGENCY = 3

# ota 开放分销端
OTA_FLIPPY_STATUS = True
OTA_CTRIP_STATUS = True
OTA_CTRIP_INTL_STATUS = True
OTA_QUNAR_STATUS = True
OTA_QUNAR_SPEC_STATUS = False
CTRIP_BETA = True

# 相同供应商
exchange_supplier = {
    42: 6,
    43: 12,
    127: 112,
    13: 105,
}

# OTA pick supplier_id weight 如果比价结果在前n里才挑选此供应商, 否则弃用
OTA_SUPPLIER_WEIGHT = {
    4: 1,
    20: 1,
    104:3,
    59:2,
    28: 2,
    116: 2,
}

OTA_BLACK_LIST = {
    'mainland': {
        'fliggy': [],
        'qunar': [],
        'ctrip': [],
        'ctrip_intl': []
    },
    'abroad': {
        'fliggy': [],
        'qunar': [],
        'ctrip': [],
        'ctrip_intl': []
    },
    'hks': {
        'fliggy': [],
        'qunar': [],
        'ctrip': [],
        'ctrip_intl': []
    }
}

OTA_WHITE_LIST = {
    'mainland': {
        'fliggy': [6,8,10,12,13,17,21,26,30,33,34,36,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
        'qunar': [6,8,10,12,13,17,21,26,30,33,34,36,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
        'ctrip': [6,8,10,12,13,17,21,26,30,33,34,36,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
    },
    'abroad': {
        'fliggy': [4,6,8,10,12,13,17,18,20,21,26,28,30,33,34,36,42,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
        'qunar': [4,6,8,10,12,13,17,18,20,21,26,28,30,33,34,36,42,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
        'ctrip': [4,6,8,10,12,13,17,18,20,21,26,28,30,33,34,36,42,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
    },
    'hks': {
        'fliggy': [4,6,8,10,12,13,17,18,20,21,26,28,30,33,34,36,42,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
        'qunar': [4,6,8,10,12,13,17,18,20,21,26,28,30,33,34,36,42,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
        'ctrip': [4,6,8,10,12,13,17,18,20,21,26,28,30,33,34,36,42,43,46,59,69,74,75,77,78,79,82,83,88,89,96,106,115,116,127,168,169,182],
    }
}

# ota 比价服务忽略地区列表
OTA_AREA_BLACK_LIST = {
    'fliggy': [],
    'qunar': [],
    'ctrip': [],
    'ctrip_intl': []
}

# FLIGGY国外酒店黑名单供应商
FLIGGY_ABROAD_BLACK_LIST = [19, 23, 32, 72, 73, 37, 125, 102, 168, 91]
# FLIGGY大陆酒店黑名单供应商
FLIGGY_MAINLAND_BLACK_LIST = [4, 19, 20, 32, 42, 72, 73, 37, 102, 125, 168, 91]
# FLIGGY港澳台酒店黑名单供应商
FLIGGY_HKS_BLACK_LIST = [4, 19, 32, 37, 72, 73, 102, 125, 168, 91]

# 携程国外酒店黑名单供应商
CTRIP_ABROAD_BLACK_LIST = [18, 19, 23, 32, 72, 73, 37, 125, 102, 168, 91, 125]
# Ctrip大陆酒店黑名单供应商
CTRIP_MAINLAND_BLACK_LIST = [4, 18, 19, 20, 32, 42, 72, 73, 37, 102, 125, 168, 91]
# Ctrip港澳台酒店黑名单供应商
CTRIP_HKS_BLACK_LIST = [4, 18, 19, 32, 37, 72, 73, 102, 125, 168, 91]

# qunar国外酒店黑名单供应商
QUNAR_ABROAD_BLACK_LIST = [18, 19, 23, 32, 72, 73, 37, 125, 102, 168, 91]
# QUNAR大陆酒店黑名单供应商
QUNAR_MAINLAND_BLACK_LIST = [4, 18, 19, 20, 32, 42, 72, 73, 37, 102, 125, 168, 91]
# QUNAR港澳台酒店黑名单供应商
QUNAR_HKS_BLACK_LIST = [4, 18, 19, 32, 37, 72, 73, 102, 125, 168, 91]

# 国家数据：104 中国大限, 107 中国台湾, 216 中国港澳
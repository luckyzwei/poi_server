#!/usr/bin/python
# -*- coding=utf-8 -*-

# ***************** 邮件相关 ****************
smtp_server = "smtp.exmail.qq.com"
mail_user = "develop_cache@haoqiao.cn"
mail_pwd = "TUkbYoxFabN99iqX"
mail_From = "develop_cache@haoqiao.cn"

# ***************** AliLog相关 ****************
ALI_END_POINT = '.cn-hangzhou-intranet.log.aliyuncs.com'
ALI_ACCESS_KEY_ID = 'LTAIumPMj7BE3INs'
ALI_ACCESS_KEY = 'bNMSNVPZaQBorrjK67Ygc9tXUeKDHD'

# ***************** codis相关 ****************
# cache-codis环境
CACHE_CODIS_SERVER_HOST = 'cache-codis.haoqiao.cn'
CACHE_CODIS_SERVER_PORT = '30000'
# 沙盒环境
CODIS_SERVER_HOST_37 = 'sandbox.haoqiao.com'
CODIS_SERVER_PORT_37 = '29002'
# 测试环境
CODIS_SERVER_HOST_78 = 'codis-test.haoqiao.com'
CODIS_SERVER_PORT_78 = '19001'

# ***************** 短信相关 *****************
ACC_MSG_URL = 'http://si.800617.com:4400/SendSms.aspx?un=bjhqkj-1&pwd=600216&mobile=%s&msg=%s'
ACC_MSG_LONG_URL = 'http://si.800617.com:4400/SendLenSms.aspx?un=bjhqkj-2&pwd=6be34f&mobile=%s&msg=%s'

mysql_ip_list = ['mysql-offline.haoqiao.com', 'rm-bp1u67k73725dnls8713.mysql.rds.aliyuncs.com', 'rm-bp1er6r8881594i46890-vpc-rw.mysql.rds.aliyuncs.com', 'mysql-cache.haoqiao.com']

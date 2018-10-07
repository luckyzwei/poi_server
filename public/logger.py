#!/usr/bin/env python
# -*- coding=utf-8 -*-



import settings
import os
import logging
import time
import getpass
import sys


# 空日志
class MyLog(object):
    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

#
# # 本地日志
# class Logger(object):
#     # 日志类
#     logger = None
#
#     # 初始化日志模块
#     @classmethod
#     def init(cls, log_path, log_level=logging.INFO, log_name=None, lock=True):
#         """
#         :type log_path: str
#         :type log_level: int
#         :type lock: bool
#         :type log_name: str
#         :return: bool
#         """
#         try:
#             # 如果目录不存在，创建目录
#             if not os.path.exists(log_path):
#                 os.mkdir(log_path)
#             print 'log_path: ', log_path
#
#             # 日志格式
#             formatter = logging.Formatter('[%(asctime)s]--[%(process)d]--[%(levelname)s]--%(message)s')
#
#             # 通用日志
#             if log_name is None:
#                 log_name = 'logger.log'
#
#             # logger
#             log_full_name = os.path.join(log_path, log_name)
#             if lock:
#                 # 加锁版
#                 logger_fh = ConcurrentRotatingFileHandler(log_full_name, "a", 100 * 1024 * 1024, 400)
#             else:
#                 # 无锁版
#                 logger_fh = TimedRotatingFileHandler(log_full_name, when='H', backupCount=400)
#
#             # 记录日志
#             logger_fh.setLevel(log_level)
#             logger_fh.setFormatter(formatter)
#             cls.logger = logging.getLogger()
#             cls.logger.setLevel(log_level)
#             cls.logger.addHandler(logger_fh)
#             cls.logger.info('%s init successful!' % log_full_name)
#
#             # 性能调优时，故意不记日志
#             # cls.logger = MyLog()
#
#             # 返回成功
#             return True
#         except:
#             print traceback.format_exc()
#             return False

#
# class AliYunLogClient(object):
#     def __init__(self, logstore,
#                  project='haoqialog',
#                  endpoint='.cn-hangzhou-intranet.log.aliyuncs.com',
#                  accessKeyId='YcNGxRB4Yu8zhnpx',
#                  accessKey='paa34K4seYG7Ew645lVlz2fXNoj5qE'):
#
#         self.project = project
#         self.logstore = logstore
#         self.client = LogClient(project + endpoint, accessKeyId, accessKey)
#
#     def save_one_log(self, topic, source, data_dict, show_log=False):
#         """
#         :type topic: string
#         :type source: string
#         :type data_dict: dict[string, string]
#         :type show_log: bool
#         """
#         try:
#             log_item_list = list()
#             """:type: list[LogItem]"""
#
#             log_item = LogItem()
#             log_item.set_time(int(time.time()))
#             log_item.set_contents(data_dict.items())
#             log_item_list.append(log_item)
#
#             request = PutLogsRequest(self.project, self.logstore, topic, source, log_item_list)
#             response = self.client.put_logs(request)
#             if show_log:
#                 response.log_print()
#             return True
#         except:
#             Logger.logger.error(traceback.format_exc())
#             return False
#
#     # 注意参数
#     def save_multi_log(self, topic, source, g_event_queue, show_log=False):
#         """
#         :type topic: string
#         :type source: string
#         :type g_event_queue: Queue
#         :type show_log: bool
#         """
#         try:
#             log_item_list = list()
#             """:type: list[LogItem]"""
#
#             # 保存一份，以防万一，写本地日志
#             tmp_dict_list = list()
#
#             # 从队列中取数据，直到取光为止
#             while True:
#                 try:
#                     data_dict = g_event_queue.get_nowait()
#                     tmp_dict_list.append(data_dict)
#                     log_item = LogItem()
#                     log_item.set_time(int(time.time()))
#                     log_item.set_contents(data_dict.items())
#                     log_item_list.append(log_item)
#                 except gevent.queue.Empty:
#                     break
#
#             # 空记录，不用写
#             if len(log_item_list) == 0:
#                 return True
#
#             # 批量写日志到阿里云
#             try:
#                 request = PutLogsRequest(self.project, self.logstore, topic, source, log_item_list)
#                 response = self.client.put_logs(request)
#                 if show_log:
#                     response.log_print()
#             except:
#                 Logger.logger.error(traceback.format_exc())
#                 # 记录失败后，则写本地日志
#                 for data_dict in tmp_dict_list:
#                     if data_dict['level'] == 'debug':
#                         Logger.logger.debug(data_dict)
#                     elif data_dict['level'] == 'info':
#                         Logger.logger.info(data_dict)
#                     elif data_dict['level'] == 'warn':
#                         Logger.logger.warn(data_dict)
#                     elif data_dict['level'] == 'error':
#                         Logger.logger.error(data_dict)
#                     elif data_dict['level'] == 'fatal':
#                         Logger.logger.fatal(data_dict)
#                     else:
#                         Logger.logger.error(data_dict)
#             return True
#         except:
#             Logger.logger.error(traceback.format_exc())
#             return False
#
#
# # 阿里日志
# class AliLog(object):
#     # 是否用阿里日志
#     __ali_log = False
#
#     # 日志级别
#     __level = None
#     """:type: int"""
#
#     # 阿里日志类
#     __logger = None
#     """:type: AliYunLogClient"""
#
#     # 阿里日志top
#     __topic = None
#     """:type: string"""
#
#     # 本机ip
#     __server_ip = None
#     """:type: string"""
#
#     # 进程pid
#     __pid = None
#     """:type: string"""
#
#     # 协程安全的queue
#     __queue = None
#     """:type: Queue"""
#
#     # 最后一次写阿里日志时间
#     __last_send_log = None
#     """:type: float"""
#
#     @classmethod
#     def __monitor_thread(cls):
#         try:
#             while True:
#                 try:
#                     # 每10秒写一次日志
#                     gevent.sleep(10)
#                     cls.__send_log()
#                 except:
#                     pass
#         except:
#             Logger.logger.error(traceback.format_exc())
#
#     # 保存日志到内存中
#     @classmethod
#     def __save_log(cls, level, msg, session_id, sequence):
#         """
#         :type level: string
#         :type msg: string
#         :type session_id: string
#         :type sequence: string
#         """
#         try:
#             data_dict = {
#                 "server_ip": cls.__server_ip,
#                 'pid': str(os.getpid()),
#                 'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f'),
#                 'level': level,
#                 'session_id': str(session_id),
#                 'sequence': str(sequence),
#                 'msg': msg,
#             }
#
#             if cls.__ali_log:
#                 # 先加入到内存中
#                 cls.__queue.put(data_dict)
#                 # 大于1000条后，马上写日志
#                 if cls.__queue.qsize() >= 1000:
#                     cls.__send_log()
#             else:
#                 # 直接记录日志到本地
#                 message = "session_id[%s], sequence[%s]: %s" \
#                           % (data_dict.get('session_id'), data_dict.get('sequence'), data_dict.get('msg'))
#                 if data_dict['level'] == 'debug':
#                     Logger.logger.debug(message)
#                 elif data_dict['level'] == 'info':
#                     Logger.logger.info(message)
#                 elif data_dict['level'] == 'warn':
#                     Logger.logger.warn(message)
#                 elif data_dict['level'] == 'error':
#                     Logger.logger.error(message)
#                 elif data_dict['level'] == 'fatal':
#                     Logger.logger.fatal(message)
#                 else:
#                     Logger.logger.error("unknown log level")
#         except:
#             Logger.logger.error(traceback.format_exc())
#
#     @classmethod
#     def __send_log(cls):
#         try:
#             cls.__last_send_log = time.time()
#             if cls.__ali_log:
#                 # 记阿里云日志
#                 cls.__logger.save_multi_log(cls.__topic, cls.__server_ip, cls.__queue)
#         except:
#             Logger.logger.error(traceback.format_exc())
#
#     @classmethod
#     def init(cls, to_ali_log=False, log_level=logging.INFO, log_project='', log_store='', topic='', endpoint='', access_key_id='', access_key=''):
#         try:
#             # 初始化公共数据
#             cls.__ali_log = to_ali_log
#             cls.__level = log_level
#             cls.__logger = AliYunLogClient(logstore=log_store, project=log_project, endpoint=endpoint,
#                                            accessKeyId=access_key_id, accessKey=access_key)
#             cls.__topic = topic
#             cls.__pid = str(os.getpid())
#             cls.__queue = Queue()
#             cls.__last_send_log = time.time()
#             eth_dict = Tools.get_eth_addr()
#             cls.__server_ip = eth_dict.get('eth0') or eth_dict.values()[0]
#
#             # 起动自动写阿里云日志线程
#             gevent.spawn(cls.__monitor_thread)
#             return True
#         except:
#             Logger.logger.error(traceback.format_exc())
#             return False
#
#     @classmethod
#     def debug(cls, msg, session_id='', sequence=''):
#         if logging.DEBUG >= cls.__level:
#             cls.__save_log("debug", msg, session_id, sequence)
#
#     @classmethod
#     def info(cls, msg, session_id='', sequence=''):
#         if logging.INFO >= cls.__level:
#             cls.__save_log("info", msg, session_id, sequence)
#
#     @classmethod
#     def warn(cls, msg, session_id='', sequence=''):
#         if logging.WARN >= cls.__level:
#             cls.__save_log("warn", msg, session_id, sequence)
#
#     @classmethod
#     def error(cls, msg, session_id='', sequence=''):
#         if logging.ERROR >= cls.__level:
#             cls.__save_log("error", msg, session_id, sequence)
#
#     @classmethod
#     def fatal(cls, msg, session_id='', sequence=''):
#         if logging.FATAL >= cls.__level:
#             cls.__save_log("fatal", msg, session_id, sequence)

class simple_log(object):
    __instance = None
    def __new__(cls):
        if cls.__instance == None:
            cls.__instance = object.__new__(cls)
            return cls.__instance
        else:
            return cls.__instance

    def __init__(self):
        root_directory = settings.LOG_PATH
        # 文件不存在就创建
        if not os.path.exists(root_directory):
            os.makedirs(root_directory)
        current_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        path = root_directory + current_date
        try:
            os.mkdir(path)
        except OSError:
            pass
        user = getpass.getuser()
        self.logger = logging.getLogger(user)
        self.logger.setLevel(logging.DEBUG)
        logFile = os.path.basename(sys.argv[0]) + '.log'
        formatter = logging.Formatter('%(asctime)-12s %(levelname)-8s %(name)-10s %(message)-12s')
        logHand = logging.FileHandler(path + '/' + logFile)
        logHand.setFormatter(formatter)
        logHand.setLevel(logging.INFO)
        logHandSt = logging.StreamHandler()
        logHandSt.setFormatter(formatter)
        self.logger.addHandler(logHand)
        self.logger.addHandler(logHandSt)

    def debug(self, msg):
        self.logger.debug(msg)
    def info(self, msg):
        self.logger.info(msg)
    def error(self, msg):
        self.logger.error(msg)
    def warn(self, msg):
        self.logger.warn(msg)
    def critical(self, msg):
        self.logger.critical(msg)

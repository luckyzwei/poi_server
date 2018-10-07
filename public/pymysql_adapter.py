#! /usr/bin/env python
# -*- coding:utf-8 -*-

import pymysql
import os
import time
import traceback
import threading
import ConfigParser
import sys
import logging

'''
   author: zhenglq 
   created at: 2017-04-13

   这是仿照mysql_adapter.py写的，原因是mysql驱动和gevent冲突，会阻塞。换成纯python写的pymysql

'''

# init logger
try:
    df_logger = logging.getLogger('pymysql_adapter')
    df_logger.setLevel(logging.INFO)
    streamhandler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    streamhandler.setFormatter(formatter)
    df_logger.addHandler(streamhandler)
    #logger.info('db-handler log init successful!')
except:
    print "Failed to init logger"
    print traceback.format_exc()
    sys.exit(1)


_DEBUG = 1

# global locker
g_lock = threading.Lock()

# const
DEFAULT_CONFIG_FILE = './mysql_adapter.conf'
DB_EXCEPTION = -1
UNICODE = type(u'a')

# mysql info
pymysql_conns = {}
pymysql_dbs = {}
pymysql_db_hosts = {}


def pymysql_init(config_file=DEFAULT_CONFIG_FILE, hosts=[], create_connections=False, logger=df_logger):
    try:
        if _DEBUG:
            logger.info("start connect db")
        # read config
        source_dir = os.getcwd()
        conf_path = os.path.split(os.path.realpath(__file__))[0]
        os.chdir(conf_path)
        config = ConfigParser.ConfigParser()
        config.read(config_file)
        os.chdir(source_dir)
        items = config.sections()
        # parse host info
        for item in items:
            host_item = dict(config.items(item))
            if int(host_item['enable']) == 0:
                continue
            # host white list
            ip = host_item['ip']
            if hosts and ip not in hosts:
                continue
            conn = None
            if create_connections:
                conn = pymysql.connect(host=ip,
                            port=int(host_item['port'],
                            user=host_item['user'],
                            passwd=host_item['passwd'],
                            charset='utf8',
                            use_unicode=True))

            pymysql_conns[ip] = conn
            db_name_list = host_item['db']

            for db_name in db_name_list.split(','):
                db_name = db_name.strip()
                if len(db_name) == 0:
                    continue

                if db_name not in pymysql_dbs:
                    pymysql_dbs[db_name] = {}
                pymysql_dbs[db_name][ip] = conn

                if db_name not in pymysql_db_hosts:
                    pymysql_db_hosts[db_name] = {}
                pymysql_db_hosts[db_name][ip] = host_item

        if _DEBUG:
            logger.info("complete connect db success")
        return True
    except:
        logger.error("except in mysql_connect: %s" % traceback.format_exc())
        return False


def _mysql_query(conn, db_name, sql, logger):
    try:
        conn.select_db(db_name)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        logger.error("mysql query except for SQL: %s" % sql)
        logger.error(traceback.format_exc())
        return DB_EXCEPTION
    return results


def _mysql_connect(host, logger):
    try:
        conn = pymysql.connect(host=host['ip'],
                               port=int(host['port']),
                               user=host['user'],
                               passwd=host['passwd'],
                               charset='utf8',
                               cursorclass = pymysql.cursors.DictCursor,
                               use_unicode=True)
        return conn
    except:
        logger.error("except in _mysql_connect:")
        logger.error(traceback.format_exc())
        return None


def pymysql_close(hosts=[], logger=df_logger):
    """mysql close, thread safe.

    Args:
        hosts: the list of host ip, do not support mutil mysql server in one host now.
               default is empth. if hosts is not empty, only host in hosts will be closed.

        logger: the handle of logging, must have info,warning and error func.
                default logger will print info to stdout.
    """
    try:
        for ip in pymysql_conns:
            if hosts and ip not in hosts:
                continue
            conn = pymysql_conns[ip]
            try:
                conn.close()
            except:
                logger.error("mysql connect close failed")
                return False
        return True
    except:
        logger.error("except in mysql_close:")
        logger.error(traceback.format_exc())
        return False


def start_transaction(db_name, ip=None, logger=df_logger):
    """start a transaction.

    Args:
        db_name: the name for physical database. the SQL statement will be
                 executed in this real DB.

        ip:   specify database ip, default is empty, if there has same db name in
                diff ips, must use this param to specify host

        logger: the handle of logging, must have info,warning and error func.
                default logger will print info to stdout.

    Return:
        if no exception, cursor will be return, else DB_EXCEPTION be return.
    """
    try:
        # get connection
        conn = _get_conn(db_name, False, ip, logger)
        if conn == None:
            return DB_EXCEPTION
        # create transaction
        conn.select_db(db_name)
        cursor = conn.cursor()
        return cursor
    except:
        logger.error('except in start_transaction:')
        logger.error(traceback.format_exc())
        return DB_EXCEPTION


def _get_conn(db_name, keep_connection, ip, logger):
    try:
        if keep_connection:
            return _get_conn_by_db_name(db_name, ip, logger)
        else:
            host = _get_host_by_db_name(db_name, ip, logger)
            if host == None:
                return None
            conn = _mysql_connect(host, logger)
            return conn
    except:
        logger.error("_get_conn except:")
        logger.error(traceback.format_exc())
        return None


def _get_conn_by_db_name(db_name, ip, logger):
    try:
        conns = pymysql_dbs[db_name]
        if len(conns) == 0:
            return None
        elif len(conns) == 1:
            conn = conns.values()[0]
        else:
            conn = conns[ip]
        return conn
    except:
        logger.error("except in _get_conn_by_db_name:")
        logger.error(traceback.format_exc())
        return None


def _get_host_by_db_name(db_name, ip, logger):
    try:
        if db_name not in pymysql_db_hosts:
            return None
        hosts = pymysql_db_hosts[db_name]
        if len(hosts) == 0:
            return None
        elif len(hosts) == 1:
            host = hosts.values()[0]
        else:
            host = hosts[ip]
        return host
    except:
        logger.error("except in _get_host_by_db_name:")
        logger.error(traceback.format_exc())
        return None


def commit_transaction(cursor, logger=df_logger):
    """commit transaction.

    Args:
        cursor:  the cursor of current db connection, one transaction one cursor.

        logger: the handle of logging, must have info,warning and error func.
                default logger will print info to stdout.

    Return:
        if no exception, executed results will be return, else DB_EXCEPTION
        be return.
    """
    try:
        conn = cursor.connection
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except:
        logger.error("except in commit_transaction:")
        logger.error(traceback.format_exc())
        return DB_EXCEPTION


def rollback_transaction(cursor, logger=df_logger):
    """commit transaction.

    Args:
        cursor:  the cursor of current db connection, one transaction one cursor.

        logger: the handle of logging, must have info,warning and error func.
                default logger will print info to stdout.

    Return:
        if no exception, executed results will be return, else DB_EXCEPTION
        be return.
    """
    try:
        conn = cursor.connection
        conn.rollback()
        cursor.close()
        conn.close()
        return True
    except:
        logger.error("except in rollback_transaction:")
        logger.error(traceback.format_exc())
        return DB_EXCEPTION


def pymysql_query(sql, db_name, keep_connection=False, ip=None, logger=df_logger):
    """mysql query, thread safe.

    Args:
        sql: the SQL statement.

        db_name: the name for physical database. the SQL statement will be
                 executed in this real DB.

        keep_connection: 1.ture, reuse connection that execute query, this connection
                           may be timeout, so you should use mysql_connect_check and
                           mysql_reconnect to handle this problem.
                         2.false, use short connection to execute query.

        ip:   specify database ip, default is empty, if there has same db name in
                diff ips, must use this param to specify host

        logger: the handle of logging, must have info,warning and error func.
                default logger will print info to stdout.

    Return:
        if no exception, executed results will be return, else DB_EXCEPTION
        be return.
    """
    try:
        # get connection
        conn = _get_conn(db_name, keep_connection, ip, logger)
        if conn == None:
            return DB_EXCEPTION

        # use lock to keep thread safe
        if keep_connection:
            g_lock.acquire()

        results = _mysql_query(conn, db_name, sql, logger)

        if keep_connection:
            # release thread lock
            g_lock.release()
        else:
            # release connection
            try:
                conn.close()
            except:
                logger.error("mysql connect close failed")
        return results
    except:
        logger.error("except in mysql_query:")
        logger.error(traceback.format_exc())
        return DB_EXCEPTION


def pymysql_transaction_query(sql, cursor, logger=df_logger):
    """mysql query.

    Args:
        sql: the SQL statement.

        cursor:  the cursor of current db connection, one transaction one cursor.

        logger: the handle of logging, must have info,warning and error func.
                default logger will print info to stdout.

    Return:
        if no exception, executed results will be return, else DB_EXCEPTION
        be return.
    """
    try:
        #logger.info('sql:%s' % sql)
        affected_rows = cursor.execute(sql)
        results = cursor.fetchall()

        # add by zhenglq 2016-05-27
        # 当返回结果集为空，则返回(long integer rows affected)影响行数
        if len(results) == 0:
            return affected_rows

    except:
        logger.error("except in mysql_query_transaction for SQL: %s" % sql)
        logger.error(traceback.format_exc())
        return DB_EXCEPTION
    return results


def pymysql_escape_string(string, logger=df_logger):
    """escape string by mysql escape string func, thread safe.

    Args:
        string: the origin string, support unicode now.

        logger: the handle of logging, must have info,warning and error func.
                default logger will print info to stdout.

    Return:
        if no exception, escape string will be return, else origin string will
        be return.
    """
    try:
        if type(string) == UNICODE:
            escape_string = pymysql.escape_string(string.encode('utf-8')).decode('utf-8')
        else:
            escape_string = pymysql.escape_string(string)
        return escape_string
    except:
        logger.error("except in mysql_escape_string:")
        logger.error(traceback.format_exc())
        return string


if __name__ == '__main__':
    if not pymysql_init():
        df_logger.error("can not connect db")
        sys.exit(1)

    test_cursor = start_transaction('tts_test')

    sql_test = '''select * from ctrip_orders where ctrip_order_id=\'%s\' for update ''' % '10000015'
    results = pymysql_transaction_query(sql_test,test_cursor)
    if results == DB_EXCEPTION:
        print "query_failed"
        df_logger.error("query failed")
    else:
        print results[0]['ctrip_order_id']

    import pdb;pdb.set_trace()
    s = pymysql_escape_string(''' '10000015' ''')
    sql_test2 = ''' update ctrip_orders set ctrip_booking_status=1 where ctrip_order_id='%s'  ''' % s
    results = pymysql_transaction_query(sql_test2,test_cursor)

    commit_transaction(test_cursor)
    sys.exit(0)



#!/usr/bin/env python
# -*- coding=utf-8 -*-


class Const(object):
    ordered_suppliers = 'ordered_suppliers'
    new_suppliers = 'new_suppliers'
    top = 'top'
    success = 'success'
    sp_hotel_code = 'sp_hotel_code'
    Export = 'Export'
    hotel_flag = 'hotel_flag'
    schedule = 'schedule'
    orders = 'orders'
    error = 'error'
    interface = 'interface'
    min_day = 'min_day'
    max_day = 'max_day'
    hq_hotel_id = 'hq_hotel_id'
    hq_city_id = 'hq_city_id'
    supplier_id = 'supplier_id'
    sp_hotel_id = 'sp_hotel_id'
    checkin = 'checkin'
    checkout = 'checkout'
    cache_type = 'cache_type'
    room_num = 'room_num'
    sp_country_code = 'sp_country_code'
    sp_city_name = 'sp_city_name'
    adult = 'adult'
    child = 'child'
    child_age = 'child_age'
    split = 'split'
    split_days = 'split_days'
    citizenship = 'citizenship'
    rate = 'rate'
    rate_filed = 'rate_field'
    group_id = 'group_id'
    rate_md5 = 'rate_md5'
    rate_data = 'rate_data'
    code = 'code'
    msg = 'msg'
    rate_list = 'rate_list'
    data = 'data'
    session_id = 'session_id'
    done_time = 'done_time'
    request = 'request'
    response_id = 'response_id'
    sp_rate_code = 'sp_rate_code'
    prices = 'prices'
    sp_hotel_id_list = 'sp_hotel_id_list'
    hq_hotel_id_list = 'hq_hotel_id_list'
    index = 'index'
    mod = 'mod'
    sp_city_code = 'sp_city_code'
    sub_supplier = 'sub_supplier'
    sequence = 'sequence'
    forcepush = 'forcepush'
    rate_refer = 'rate_refer'
    room_id = 'room_id'
    price_code = 'price_code'
    price_key = 'price_key'
    price_ext_data = 'price_ext_data'
    third_party = 'third_party'
    name = 'name'
    desc = 'desc'
    interface_amount = 'interface_amount'
    interface_tax = 'interface_tax'
    interface_days = 'interface_days'
    interface_currency = 'interface_currency'
    child_age_limit = 'child_age_limit'
    bed_type = 'bed_type'
    bed_name = 'bed_name'
    bed_desc = 'bed_desc'
    description = 'description'
    price_factor = 'price_factor'
    price_sale = 'price_sale'
    price_days = 'price_days'
    currency = 'currency'
    interface_rate = 'interface_rate'
    pre_rate = 'pre_rate'
    cancel = 'cancel'
    on_request = 'on_request'
    room_type_id = 'room_type_id'
    property = 'property'
    recent = 'recent'
    refer_price = 'refer_price'
    room_name = 'room_name'
    count = 'count'
    qps = 'qps'
    total_qps = 'total_qps'
    online_status = 'online_status'
    green = 'green'
    web1 = 'web1'
    unknown = 'unknown'
    omit_times = 'omit_times'
    compare = 'compare'

    c_type = 'c_type'   # hotel, hotellist, bclient
    hotellist = 'hotellist'
    hotel = 'hotel'
    bclient = 'bclient'

    r_type = 'r_type'   # price ,schedule_price, price_list, schedule_pricelist
    price = 'price'
    schedule_price = 'schedule_price'
    price_list = 'price_list'
    schedule_pricelist = 'schedule_pricelist'
    check_price = 'check_price'
    update_supplier = 'update_supplier'
    get_supplier_summary = 'get_supplier_summary'
    get_online_suppliers = 'get_online_suppliers'

    high = 'high'
    normal = 'normal'
    supreme = 'supreme'
    url = 'url'
    froms = 'from'
    channel = 'channel'
    DB_SUPPLIERS = 'schedule'
    schedule_test = 'schedule_test'
    price_list_to_cache = 'price_list_to_cache'
    api = 'api'

    used_time = 'used_time'
    sum = 'sum'
    # 套餐个数
    rates_num = 'rates_num'

    average_request_time = 'average_request_time'
    actually_qps = 'actually_qps'
    error_summary = 'error_summary'
    have_rooms = 'have_rooms'
    no_room = 'no_room'
    full_room_rate = 'full_room_rate'
    sp_error = 'sp_error'
    http_error = 'http_error'
    time_out = 'time_out'
    url_error = 'url_error'
    supplier_err_rate = 'supplier_err_rate'
    http_err_rate = 'http_err_rate'
    inner_error = 'inner_error'
    config_qps = 'config_qps'
    redis_q = 'redis_q'
    have_rate = 'have_rate'
    avg_request_time = 'avg_request_time'

    except_rate = 'except_rate'
    except_qps = 'except_qps'
    limit = 'limit'
    quoted = 'quoted'
    update_time = 'update_time'
    summary = 'summary'
    flag = 'flag'
    distributor_id = 'distributor_id'
    hotels = 'hotels'
    dates = 'dates'
    action = 'action'
    specify = 'specify'
    level = 'level'
    suppliers = 'suppliers'
    offset = 'offset'
    offset_start = 'offset_start'
    offset_end = 'offset_end'
    hotels_info = 'hotels_info'
    price_num = 'price_num'
    regular_room_name = 'regular_room_name'
    min_price = 'min_price'
    # 规整房型
    regular_rooms = 'regular_rooms'

    task_type = 'task_type'
    ota = 'ota'
    # 当前已上线供应商
    currentsupplier = 'currentsupplier'
    # 供应商黑名单
    supplier_blacklist = 'supplier_blacklist'
    city_id = 'city_id'


class ErrorMsg(object):
    OK = 'success'
    PARAM_ERROR = 'invalid parameters'
    TRACE_ERROR = 'except error'
    CACHE_EMPTY = 'cache is empty'
    EXPIRE_ERROR = 'cache is expired'
    ADD_REQUEST_ERR = 'EXEC add_request error'
    GET_TIMEOUT_FROM_HQS = 'SYNC response timeout'
    SCHEDULE_OK = 'Schedule ok'
    GET_SUPPLIER_FROM_RATE = 'get supplier_id error from sp_rate_code'
    GET_HTTP_ERR_FROM_HQS = 'get http error from hqs'
    DOWNLOAD_INNER_ERROR = 'download server inner error'
    HQS_RESULT_LOADS_ERROR = 'loads hqs response error'


class ErrorCode(object):
    OK = 0
    CACHE_EMPTY = -1
    LOGIC_ERROR = -2
    TRACE_ERROR = -3
    PARAM_ERROR = -4
    EXPIRE_ERROR = -5

    failed_flag = -1
    online_flag = 0
    offline_flag = 1
    total_reload_flag = 2
    success_flag = 0

    ADD_REQUEST_ERR = -1050
    URL_ERR = -1550
    GET_HTTP_ERR_FROM_HQS = -1551
    GET_TIMEOUT_FROM_HQS = -1553
    GET_SUPPLIER_FROM_RATE = -1253
    DOWNLOAD_INNER_ERROR = -1552

    HQS_HAVE_ROOM = 0
    HQS_NO_ROOM = -2
    HQS_SUPPLIER_ERR = -3
    HQS_RESULT_LOADS_ERROR = -4


class RetCode(object):
    OK = 0
    param_error = -1
    error = -2
    flag_error = -3
    repeat_request = -4
    inner_error = -10
    interface_cancel = -11


class RetMsg(object):
    msg = {
        RetCode.OK: 'success',
        RetCode.error: 'error, please check',
        RetCode.param_error: 'param_error',
        RetCode.inner_error: 'inner_error',
        RetCode.repeat_request: 'repeat_request ignore',
        RetCode.flag_error: 'operate flag error[0:online,1:offline]',
        RetCode.interface_cancel: 'interface have cancelled',
    }

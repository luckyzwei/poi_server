#!/usr/bin/env python
# -*- coding=utf-8 -*-

import urllib
import urllib2


class MyRequest(object):
    headers_useless = ['User-Agent', 'user-agent']

    @classmethod
    def request(cls, url, param, headers):
        for item in cls.headers_useless:
            if item in headers:
                headers.pop(item)
        req = urllib2.Request('%s?%s' % (url, urllib.urlencode(param)))
        for k, v in headers.items():
            req.add_header(k, v)
        res = urllib2.urlopen(req)
        return res.read(), dict(res.info())

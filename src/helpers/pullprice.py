#!/usr/bin/env python
#coding:utf-8
# Author:   --<>
# Purpose: 
# Created: 2012-09-05

import socket
import logging
import urllib.parse
import urllib.request

# timeout in seconds
timeout = 10
socket.setdefaulttimeout(timeout)

def pull_price(code, startd=None, endd=None, period='d'):
    values={}
    values['s']=code
    if startd:
        values['a']=startd.month - 1
        values['b']=startd.day
        values['c']=startd.year
    if endd:
        values['d']=endd.month - 1
        values['e']=endd.day
        values['f']=endd.year
    values['g']=period
    
    url=r'http://ichart.finance.yahoo.com/table.csv'
    data = urllib.parse.urlencode(values)
#    usr_agent='Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)'
#    header={'User-Agent':usr_agent}
#    req = urllib.request.Request(url+'?'+data, headers=header)
    req = urllib.request.Request(url+'?'+data)
    try:
        response = urllib.request.urlopen(req)
        return response.read().decode()
    except Exception as e:
        logging.debug(str(e.__class__) + str(e))
        logging.debug(req.full_url)
        return False

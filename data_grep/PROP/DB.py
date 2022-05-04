# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 09:52:59 2019

@author: taisport
"""

import mysql.connector
from mysql.connector import Error

Hostname = 'WTWONG-ADD-2'

def DB_PERSONAL():
    connection = mysql.connector.connect(host=Hostname, user="personal",passwd="24349179", database="personal", charset='utf8mb4')
    return(connection)
    
def DB_STOCK():
    connection = mysql.connector.connect(host=Hostname, user="stock",passwd="24349179", database="stock", charset='utf8mb4')
    return(connection)

def DB_STOCK_HK():
    connection = mysql.connector.connect(host=Hostname, user="stock_hk",passwd="24349179", database="stock_hk", charset='utf8mb4')
    return(connection)

def DB_CRYPTO():
    connection = mysql.connector.connect(host=Hostname, user="taisport",passwd="24349179", database="crypto", charset='utf8mb4')
    return(connection)

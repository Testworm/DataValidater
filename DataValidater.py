#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author : Torre Yang Edit with Python3.6
# @Email  : klyweiwei@163.com
# @Time   : 2018/10/24 16:50
# 自动扫描表数据，筛选异常数据;
# 实现方法： 筛选规则：
# 1.标签字段设置阈值：每个标签字段的阈值不同, 需要规定每个字段的阈值。
# select * from table where column > a and column < b
# 2.标签字段全为空数据
# select count(distinct column) from table;
# if 结果=1/0, 则异常结果
# 3.数据重复检查, 表结构设置主键; 直接筛选出来fundAccount
# select count(distinct fundAccount) from table;
# 数据过多 可以分页查询
# 数据检查, 可以利用JMeter将筛选的数据, 利用java比对脚本校验;

# 导出表字段核对：
import connect_dataBase
import logging

PASS = ": PASS, 通过"
NOTPASS = "测试不通过"
# 写入文件
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='./stdout.log',
                    filemode='w')
# 第一步, db连接
server = "mysql"
try:
    connectDB = connect_dataBase.ConnectDatabase()
    get_conf = connectDB.get_conf('databases_conf.json')
    conn, cur = connectDB.connect_db(get_conf[server]["host"], get_conf[server]["user"],
                                     get_conf[server]["password"], get_conf[server]["database"],
                                     get_conf[server]["port"])
except:
    print('异常登陆, 请检查配置信息')

# 第二步, 拿到库中所有表，存储到List, 可以添加筛选条件, 比如只存带portrait前缀的表
dbSql = "show tables"
tablesRes = connectDB.get_res(cur, dbSql)
# 元组转存list
tables = []
for i in range(len(tablesRes)):
    if "data_" in tablesRes[i][0]:   # 设置需要过滤的表
        tables.append(tablesRes[i][0])
    else:
        print("过滤此表")
logging.info("表收集完毕")
logging.info(tables)

tableCols = {}  # key 为table名, value是字段list
# 获取每个表的字段, 存储到列表, 以表名 命名
for i in range(len(tables)):
    columnSql = "desc {0}".format(tables[i])
    print(columnSql)
    columnRes = connectDB.get_res(cur, columnSql)
    # tables[i] = []
    cols = []
    for ii in range(len(columnRes)):
        # tables[i].append(columnRes[ii][0])
        cols.append(columnRes[ii][0])
    tableCols[tables[i]] = cols
    # print(tables[i])
    # logging.info(tables[i])
    print(tableCols)


# 第三步: 建立规则, 1.此规则属于每个表 2.此规则属于某个表
# 1. 所有表字段不为空值 , 表字段格式化, 筛选出null的表+字段
# for i in range(len(tables)): # 表
#     # for ii in range(len(tables[i])): # 字段
#         for col in tables[i]:
#             NullSql = "select {0} from {1} where {0} is null".format(col, tables[i])
for table, cols in tableCols.items():
    logging.info("----------" + table + "-----------")
    for col in cols:
        NullSql = "select * from {0} where {1} is null".format(table, col)
        NullRes = connectDB.get_res(cur, NullSql)
        # print(len(NullRes)
        if len(NullRes) > 0:
            print(table + " " + col + ": 此字段包含异常值, 请进一步排查" + NOTPASS)
            logging.info(table + " " + col + ": 此字段包含异常值, 请进一步排查" + NOTPASS)
        else:
            print(col + PASS)
            logging.info(col + PASS)
    logging.info("----------" + table + "-----------")
# 2.筛选选为null的字段
for table, cols in tableCols.items():
    logging.info("----------" + table + "-----------")
    for col in cols:
        NullSql = "select count(distinct {0}) from {1}".format(col, table)
        NullRes = connectDB.get_res(cur, NullSql)
        print(NullRes[0][0])
        if NullRes[0][0] == 0 or NullRes[0][0] == 1:
            print(table + " " + col + ": 此字段可能为空值 请进一步排查" + NOTPASS)
            logging.info(table + " " + col + ": 此字段可能为空值, 请进一步排查" + NOTPASS)
        else:
            print(col + PASS)
            logging.info(col + PASS)
    logging.info("----------" + table + "-----------")






























#coding:utf-
import sys
sys.path.append("..")
import pymysql
from common.mysql_conf_api import MySQLConfigApi
import datetime
import pickle
import logging
import traceback
import gc
import os
from common.utils import *

url = "https://pic.lehe.com/pic/_o/67/6d/7ab70c5e5b80848772cc1d97cfd5_900_900.cz.jpg"

def get_positive_data():
    #取1000个
    #过去一个月下过三个订单以上的商品，价格高于500元的商品作为正样本
    h, p, u, pwd, d = MySQLConfigApi.get_param_from_ini_file('higo_order', 0, False)
    db_order = pymysql.connect(host=h, port=int(p), user=u, passwd=pwd, db=d).cursor()

    h, p, u, pwd, d = MySQLConfigApi.get_param_from_ini_file('higo_goods', 0, False)
    db_goods = pymysql.connect(host=h, port=int(p), user=u, passwd=pwd, db=d).cursor()
    h, p, u, pwd, d = MySQLConfigApi.get_param_from_ini_file('higo_base', 0, False)
    db_base = pymysql.connect(host=h, port=int(p), user=u, passwd=pwd, db=d).cursor()


    sql = """select goods_id, cnt, sku_final_price  from ( 
    select
    goods_id, sku_final_price, count(goods_id) as cnt
    from (
    select 
        goods_id, 
        sku_final_price,
        goods_image 
    from t_pandora_order_item
    where 
        ctime > '2017-04-11' and ctime < '2017-07-11'
      ) t
      where sku_final_price > 500
      group by goods_id, goods_image
      ) tt where tt.cnt > 5 limit 1000"""
    goods_dict = {}
    try:
        db_order.execute(sql)
        for r in db_order.fetchall():
            goods_id = r[0]
            sql = "select main_img_id from t_pandora_goods where goods_id=%s" % (goods_id)
            db_goods.execute(sql)
            for g in db_goods.fetchall():
                main_img_id = g[0]
                sql = "select higo_path from t_pandora_image where image_id=%s" % main_img_id
                db_base.execute(sql)
                for i in db_base.fetchall():
                    higo_path = 'https://pic.lehe.com/' + i[0]
                    goods_dict.update({'goods_id' : goods_id, 'higo_path' : higo_path})
        print(goods_dict)

    except Exception as e:
        traceback.print_exc()
    finally:
        db_order.close()
        db_goods.close()

    return

def get_negative_data():
    #取2400个
    #过去三个月没有过订单没有过点赞的商品，
    host, port, user, pwd, db = MySQLConfigApi.get_param_from_ini_file('higo_goods', 0, False)
    db = torndb.Connection(host + ':' + port, db, user, pwd)

    return

def main():
    get_positive_data()
    #get_negative_data()
    return

if __name__ == "__main__":
    main()

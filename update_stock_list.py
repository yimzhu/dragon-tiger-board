#coding=utf-8
#Author: yimzhu(yiming0330@gmail.com)
#Created: 20170603
#配合十大股东维护stock表用
import tushare
from sqlalchemy import create_engine
import sqlalchemy
import pandas
import json

def update_sinfo_into_table(sql_conn_string, year, quarter):
     #从tushare获取上市公司股票代码数据
     df=tushare.get_stock_basics()
     dbconn_json = json.loads(sql_conn_string)
     
     column_to_be_del_list=('industry','area','pe','outstanding','totals','totalAssets',
                            'liquidAssets','undp','perundp','rev',
                            'profit','fixedAssets','reserved','reservedPerShare',
                            'esp','bvps','pb','gpr','npr')
     
     for del_item in column_to_be_del_list:
          del df[del_item]
          
     # 打开数据库连接
     engine = create_engine("mysql+pymysql://"+dbconn_json['username']+":"+dbconn_json['password']
                            +"@"+dbconn_json['ip']+"/"+dbconn_json['dbname']+"?charset=utf8",echo=True)
     engine.execute('truncate stock')
     df.to_sql(name='stock',con=engine,if_exists='append',dtype={'code':sqlalchemy.types.NVARCHAR(length=255),
                  'name':sqlalchemy.types.NVARCHAR(length=255),
                  'timeToMarket':sqlalchemy.NVARCHAR(length=255),
                  'holders':sqlalchemy.types.BIGINT()
           })     
     return None

sql_conn_string = '{"ip":"localhost","username":"root","password":"password","dbname":"dt"}'
year = '2017'
quarter = 1

update_sinfo_into_table(sql_conn_string,year,quarter)
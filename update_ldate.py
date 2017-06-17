#coding=utf-8
#Author: yimzhu(yiming0330@gmail.com)
#Created: 20170603
#步骤X：如果前两个步骤没有按照时间线来获取数据，则通过该脚本更新agency表
#把agency表中最后一次上榜日期与daily表中最新的日期数据同步，如数据不存在则插入
#仅在往数据库插入历史数据的时候同步用

import pymysql
from sqlalchemy import create_engine
import time,datetime

host = 'localhost'
username = 'root'
password = 'password'
db_name = 'dt'

#'2017-05-17'
select_all_agencies = 'select distinct(agname) from daily'
select_specific_agency = 'select count(1) from daily where agname = %s'
select_agency_more_than_one = 'select count(1) from daily where agname = %s'
select_lastest_date = 'select max(date) from daily where agname = %s'
update_ldate = 'update agency set ldate = %s where agname = %s'
select_ldate = 'select ldate from agency where agname = %s'
insert_agency = 'insert agency(agname,ldate) values(%s,%s)'

# 打开数据库连接
connection = pymysql.connect(host=host,user=username,password=password,charset='utf8mb4',db=db_name)

with connection.cursor() as cursor:
    cursor.execute(select_all_agencies)
    all_agencies_list = cursor.fetchall()
    
    #print(all_agencies_list)
    agency_index = 0
    
    while agency_index != len(all_agencies_list)-1:
        #print(all_agencies_list[agency_index][0])select_lastest_date
        cursor.execute(select_agency_more_than_one,(all_agencies_list[agency_index][0]))
        #print(all_agencies_list[agency_index][1])
        selected_agency_result = cursor.fetchone()[0]
        cursor.execute(select_specific_agency,(all_agencies_list[agency_index][0]))
        agency_exist = cursor.fetchone()[0]
        #print(agency_exist)
        if agency_exist != 'None':
            #print(all_agencies_list[agency_index][0])
            cursor.execute(select_lastest_date,(all_agencies_list[agency_index][0]))
            daily_ldate = cursor.fetchone()[0]
            cursor.execute(select_ldate,(all_agencies_list[agency_index][0]))
            agency_ldate = cursor.fetchone()[0]
            if daily_ldate != agency_ldate:
                cursor.execute(update_ldate,(daily_ldate,all_agencies_list[agency_index][0]))
                connection.commit()
                #print('agency表[' + all_agencies_list[agency_index][0] + ']的日期:' + str(agency_ldate))
                print('更新agency表 [' + all_agencies_list[agency_index][0] + '] 为：' + str(daily_ldate))       
        else:
            cursor.execute(select_lastest_date,(all_agencies_list[agency_index][0]))
            daily_ldate = cursor.fetchone()[0]            
            cursor.execute(insert_agency,(all_agencies_list[agency_index][0],daily_ldate))
            connection.commit()
            print('插入agency表: 营业部[' + all_agencies_list[agency_index][0] + ']，日期[' + daily_ldate + ']')
        agency_index = agency_index + 1       

connection.close()

'''
try:
    with connection.cursor() as cursor:
        cursor.execute(select_all_agencies)
        all_agencies_list = cursor.fetchall()
        #print(all_agencies_list)
        agency_index = 0
        
        while agency_index != len(all_agencies_list)-1:
            cursor.execute(select_agency_to_be_updated,(all_agencies_list[agency_index][1],all_agencies_list[agency_index][0]))
            #print(all_agencies_list[agency_index][1])
            selected_agency_result = cursor.fetchall()
            print(all_agencies_list[agency_index][1] + ':' + len(selected_agency_result))
            agency_index = agency_index + 1
            connection.commit()
except:
    print('数据库操作异常！')
finally:
    connection.close()
'''
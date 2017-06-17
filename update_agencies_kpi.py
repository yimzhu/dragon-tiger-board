#coding=utf-8
#Author: yimzhu(yiming0330@gmail.com)
#Created: 20170603
#步骤4：计算营业部胜率

import os,time,datetime,pymysql

def updateall_agency_kpi():
    '''
    string型日期转换date型日期
    :param: string类型格式日期，
    :param: 输入日期的格式，如%Y%m%d
    :return: date类型日期 
    '''
    select_daily_sum = 'SELECT count(1) FROM daily WHERE agname = %s'
    select_daily_t2_failure = "SELECT count(1) FROM daily WHERE agname = %s AND t2_kpi LIKE '%-%'"
    select_daily_t5_failure = "SELECT count(1) FROM daily WHERE agname = %s AND t5_kpi LIKE '%-%'"
    
    update_agency_t2kpi = 'update agency set t2_kpi = %s WHERE agnanme = %s'
    update_agency_t5kpi = 'update agency set t5_kpi = %s WHERE agnanme = %s'
    update_agency_sum = 'update agency set sum = %s where agname = %s'
    #update_agency_ = 'update agency set t5_kpi = %s where agnanme = %s'
    
    select_agencyname = 'SELECT agname FROM agency'
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(select_agencyname)
            agency_list = cursor.fetchall()
            #print(agency_list)
            agency_lish_length = len(agency_list)
            #print(agency_lish_length)
            agency_index = 0
            
            while agency_index != len(agency_list) -1:
                cursor.execute(select_daily_t2_failure,(agency_list[agency_index]))
                agency_t2_failure_sum = cursor.fetchone()[0]
                
                cursor.execute(select_daily_t5_failure,(agency_list[agency_index]))
                agency_t5_failure_sum = cursor.fetchone()[0]
                
                cursor.execute(select_daily_sum,(agency_list[agency_index]))
                agency_all_sum = cursor.fetchone()[0]
                
                agency_t2_success_sum = agency_sum - agency_t2_failure_sum
                agency_t2_success_rate = agency_t2_success_sum/agency_all_sum
                
                agency_t5_success_sum = agency_sum - agency_t5_failure_sum
                agency_t5_success_rate = agency_t5_success_sum/agency_all_sum            
                
                cursor.execute(update_agency_t2kpi,(agency_t2_success_rate,agency_list[agency_index]))
                cursor.execute(update_agency_t5kpi,(agency_t5_success_rate,agency_list[agency_index]))
                cursor.execute(update_agency_sum,(agency_all_sum,agency_list[agency_index]))
                print('更新[' + agency_list[agency_index] + '], 第2天胜率[' + agency_t2_success_rate + ']，第5天胜率[' + agency_t5_success_rate + ']')
                agency_index = agency_index + 1
        connection.commit()
    except:
        print('数据库写入错误')
    return None

host = 'localhost'
username = 'root'
password = 'password'
db_name = 'dt'

# 打开数据库连接
connection = pymysql.connect(host=host,user=username,password=password,charset='utf8mb4',db=db_name)

#updateall_agency_kpi()

select_daily_sum = 'SELECT count(1) FROM daily WHERE agname = %s'
select_daily_t2_failure = "SELECT count(1) FROM daily WHERE agname = %s AND t2_kpi REGEXP '^[-]'"
select_daily_t5_failure = "SELECT count(1) FROM daily WHERE agname = %s AND t5_kpi REGEXP '^[-]'"

update_agency_t2kpi = 'update agency set t2kpi = %s WHERE agname = %s'
update_agency_t5kpi = 'update agency set t5kpi = %s WHERE agname = %s'
update_agency_sum = 'update agency set sum = %s where agname = %s'
#update_agency_ = 'update agency set t5_kpi = %s where agnanme = %s'

select_agencyname = 'SELECT agname FROM agency'

with connection.cursor() as cursor:
    cursor.execute(select_agencyname)
    agency_list = cursor.fetchall()
    #print(agency_list)
    agency_lish_length = len(agency_list)
    #print(agency_lish_length)
    agency_index = 0
    
    while agency_index != len(agency_list) -1:
        print(agency_list[agency_index][0])
        cursor.execute(select_daily_t2_failure,(agency_list[agency_index][0]))
        agency_t2_failure_sum = cursor.fetchone()[0]
        
        #print(agency_t2_failure_sum)
        
        cursor.execute(select_daily_t5_failure,(agency_list[agency_index][0]))
        agency_t5_failure_sum = cursor.fetchone()[0]
        
        
        cursor.execute(select_daily_sum,(agency_list[agency_index][0]))
        agency_all_sum = cursor.fetchone()[0]
        #print(agency_all_sum)
        
        agency_t2_success_sum = agency_all_sum - agency_t2_failure_sum
        agency_t2_success_rate = (agency_t2_success_sum/agency_all_sum)*100
        #print(agency_t2_success_rate)
        agency_t5_success_sum = agency_all_sum - agency_t5_failure_sum
        agency_t5_success_rate = (agency_t5_success_sum/agency_all_sum)*100       
        
        print(agency_list[agency_index][0])
        cursor.execute(update_agency_t2kpi,(agency_t2_success_rate,agency_list[agency_index][0]))
        cursor.execute(update_agency_t5kpi,(agency_t5_success_rate,agency_list[agency_index][0]))
        cursor.execute(update_agency_sum,(agency_all_sum,agency_list[agency_index]))
        print('更新[' + agency_list[agency_index][0] + '], 第2天胜率[' + str(agency_t2_success_rate) + ']，第5天胜率[' + str(agency_t5_success_rate) + ']')
        agency_index = agency_index + 1
connection.commit()

connection.close()

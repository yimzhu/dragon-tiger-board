#coding=utf-8
#Author: yimzhu(yiming0330@gmail.com)
#Created: 20170603
#步骤2：读取csv文件，从接口读取t2t5获取价格信息写入daily表，更新agency表
import urllib.request,urllib.parse,json,os,time,datetime,http.cookiejar,string,os,sys,pymysql,re,tushare

def get_date_from_filename(strdate):
    '''
    %Y%m%d 格式转换为 %Y-%m-%d
    :param: string类型格式日期 %Y%m%d
    :return: string类型格式日期 %Y-%m-%d
    '''     
    gd_timearray = time.strptime(strdate, "%Y%m%d")
    gd_timestamp = int(time.mktime(gd_timearray))
    gd_date = datetime.datetime.fromtimestamp(gd_timestamp)
    return gd_date.strftime("%Y-%m-%d")

def change_strdate_to_date(strdate,strdatetype):
    '''
    string型日期转换date型日期
    :param: string类型格式日期，
    :param: 输入日期的格式，如%Y%m%d
    :return: date类型日期
    '''     
    cs_timearray = time.strptime(strdate, strdatetype)
    cs_timestamp = int(time.mktime(cs_timearray))
    cs_day = datetime.datetime.fromtimestamp(cs_timestamp)
    return cs_day

def get_strdate_before_n_tdays(date,ndaysbefore):
    '''
    获取T-n个交易日的日期
    :param: 日期型日期，第n天前
    :return: T-n个交易日的日期
    ''' 
    index_row = 0
    passed_transcation_dates = ''
    full_transcation_dates = ''
    #获取全部日期
    df = tushare.trade_cal()
    #筛选出交易日
    df = df[df['isOpen']==1]    
    for i,row in df.iterrows():
        df_timearray = time.strptime(row['calendarDate'], "%Y-%m-%d")
        df_timestamp = int(time.mktime(df_timearray))
        df_day = datetime.datetime.fromtimestamp(df_timestamp)
        
        full_transcation_dates = full_transcation_dates + df_day.strftime("%Y-%m-%d") + ','
        
        if (df_day <= date):
            passed_transcation_dates = passed_transcation_dates + df_day.strftime("%Y-%m-%d") + ','
            index_row = index_row + 1    
    passed_transcation_dates_lists = passed_transcation_dates.split(',')
    full_transcation_dates_lists = full_transcation_dates.split(',')
    passed_transcation_dates_lists_length = len(passed_transcation_dates_lists)-1
    full_transcation_dates_lists_length = len(full_transcation_dates_lists)-1
    return full_transcation_dates_lists[passed_transcation_dates_lists_length-1-int(ndaysbefore)]

def get_strdate_after_n_tdays(date,ndaysafter):
    '''
    获取T-n个交易日的日期
    :param: date 日期型日期
    :param: ndaysafter 第n天后
    :return: T-n个交易日的日期
    '''    
    index_row = 0
    passed_transcation_dates = ''
    full_transcation_dates = ''
    #获取全部日期
    df = tushare.trade_cal()
    #筛选出交易日
    df = df[df['isOpen']==1]    
    for i,row in df.iterrows():
        df_timearray = time.strptime(row['calendarDate'], "%Y-%m-%d")
        df_timestamp = int(time.mktime(df_timearray))
        df_day = datetime.datetime.fromtimestamp(df_timestamp)
        
        full_transcation_dates = full_transcation_dates + df_day.strftime("%Y-%m-%d") + ','
        
        if (df_day <= date):
            passed_transcation_dates = passed_transcation_dates + df_day.strftime("%Y-%m-%d") + ','
            index_row = index_row + 1    
    passed_transcation_dates_lists = passed_transcation_dates.split(',')
    full_transcation_dates_lists = full_transcation_dates.split(',')
    passed_transcation_dates_lists_length = len(passed_transcation_dates_lists)-1
    full_transcation_dates_lists_length = len(full_transcation_dates_lists)-1
    return full_transcation_dates_lists[passed_transcation_dates_lists_length-1+int(ndaysafter)]

host = 'localhost'
username = 'root'
password = 'password'
db_name = 'dt'

# 打开数据库连接
connection = pymysql.connect(host=host,user=username,password=password,charset='utf8mb4',db=db_name)
#connection.autocommit()
opfolder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/output'

def get_filename(dir, fileList):
    newDir = dir
    if os.path.isfile(dir):
        fileList.append(dir)
    elif os.path.isdir(dir):  
        for s in os.listdir(dir):
            #如果需要忽略某些文件夹，使用以下代码
            #if s == "xxx":
                #continue
            newDir=os.path.join(dir,s)
            get_filename(newDir, fileList)  
    return fileList

insert_daily_sql = """\
    INSERT INTO daily(date,agname,scode,sname,border,amount,t_price)
     VALUES(%s,%s,%s,%s,%s,%s,%s)
    """
select_daily_sql = 'select count(*) from daily where scode = %s and date = %s and agname = %s'
insert_agency_sql = 'insert into agency(agname,ldate) values(%s,%s)'
select_agency_sql = 'select count(*) from agency where agname = %s'
update_agency_sql = 'update agency set ldate = %s where agname = %s'

#遍历output目录，获得要导入数据库数据文件的名称
output_lists = get_filename(opfolder, [])

for output_list in output_lists:
    if not 'DS' in output_list and not 'thumb' in output_list:
        file_fullname = output_list.split('/')[6]
        file_nameonly = file_fullname.split('.')[0]
        #print(file_nameonly)
        fn_timearray = time.strptime(file_nameonly, "%Y%m%d")
        fn_timestamp = int(time.mktime(fn_timearray))
        the_specific_day = datetime.datetime.fromtimestamp(fn_timestamp) 
        the_day_after_two_days = get_strdate_after_n_tdays(the_specific_day,2)
        the_day_before_five_days = get_strdate_after_n_tdays(the_specific_day,5)
        
        f = open(output_list, 'rt', encoding='gbk') 

        try:
            with connection.cursor() as cursor:
                for line in f: 
                    dblist = re.sub('\n','',line).split(',') 
                    
                    if '买入' in dblist[3] and not '融资' in dblist[3]:
                        lineindex = 0
                        border = 0
                     
                        for cell in dblist:
                            if int(lineindex) > 3 and not '.' in cell:
                                border=int(border)+1
                                cursor.execute(select_daily_sql, (dblist[1],dblist[0],dblist[lineindex]))
                                r_daily_exists = cursor.fetchone()[0]
                                if r_daily_exists == 0:
                                    t_info = tushare.get_k_data(dblist[1],start=str(dblist[0]),end=str(dblist[0]))['close'].values
                                    t_price = t_info[0]
                                    #cursor.execute(insert_daily_sql,(dblist[0],dblist[lineindex],dblist[1],dblist[2],int(border),dblist[lineindex+1],str(t_price)))
                                    cursor.execute(insert_daily_sql,(dblist[0],dblist[lineindex],dblist[1],dblist[2],int(border),dblist[lineindex+1],str(t_price)))
                                    print('插入daily表:' + dblist[0] + ', 股票代码:' + dblist[1] + ', 营业部名称：' + dblist[lineindex])
                                    cursor.execute(select_agency_sql,(dblist[lineindex]))
                                    #判断数据是否存在
                                    r_agency_exists = cursor.fetchone()[0]
                                    if r_agency_exists == 0:
                                        cursor.execute(insert_agency_sql,(dblist[lineindex],dblist[0]))
                                        print('插入agency表：营业部名[' + dblist[lineindex] + '],' + '日期[' + dblist[0] + ']')
                                    elif r_agency_exists != 0:
                                        cursor.execute(update_agency_sql,(dblist[0],dblist[lineindex]))
                                        print('更新' + dblist[lineindex] + '的日期为:' + dblist[0])
                                else:
                                    print('忽略重复数据:'+ dblist[0] + ',股票代码:' + dblist[1] + '，营业部名称：' + dblist[lineindex])
                            lineindex=lineindex+1  
                    connection.commit()
        except pymysql.OperationalError as e:
            if e.errno == 2006:
                print("数据库失去连接")
            else:
                # 发生错误时回滚
                connection.rollback()
connection.close()
print('操作结束。')
        #birthday=datetime.date.today() 
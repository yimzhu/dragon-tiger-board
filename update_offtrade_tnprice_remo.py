#coding=utf-8
#Author: yimzhu(yiming0330@gmail.com)
#Created: 20170603
#步骤2：更新第n日的price和kpi
import urllib.request,urllib.parse,json,os,time,datetime,http.cookiejar,string,os,sys,pymysql,re,tushare

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

host = '192.168.100.224'
username = 'root'
password = 'password'
db_name = 'dt'

# 打开数据库连接
connection = pymysql.connect(host=host,user=username,password=password,charset='utf8mb4',db=db_name)

def update_tnprice(connection, maxday):
    '''
    g
    :param: connection 
    :param: tnday 只能为 2 或 5
    :return: None
    '''        
    #存处理过的买入数据
    opfolder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/logs'    
    #记录和原始数据库tprice不一致的日志
    savedlogs = opfolder + '/' + 'updateprice.log'
    
    #select_daily_offtrade_sql = 'select distinct(scode),sname,date from daily where t2_date is null and offtrade = 1 and offdate is null／／／／ and offdate is not null order by date desc'
    select_daily_offtrade_sql = 'select distinct(scode),sname,date,t2_date,t2_price,t5_date from daily where offtrade is null order by date desc'
    update_daily_offtrade_sql = 'update daily set offtrade = %s where scode = %s and date = %s'
    
    select_daily_offdate_sql = 'select offdate from daily where scode where scode = %s and date = %s'
    update_daily_offdate_sql = 'update daily set offdate = %s where scode = %s and date = %s'
    select_daily_tprice_sql = 'select t_price from daily where scode = %s and date = %s'
    update_daily_tprice_sql = 'update daily set t_price = %s where scode = %s and date = %s'
    
    update_daily_t2price_sql = 'update daily set t2_price = %s where scode = %s and date = %s'
    update_daily_t2date_sql = 'update daily set t2_date = %s where scode = %s and date = %s'
    update_daily_t2_kpi_sql = 'update daily set t2_kpi = %s where scode = %s and date = %s'
    update_daily_t5price_sql = 'update daily set t5_price = %s where scode = %s and date = %s'
    update_daily_t5date_sql = 'update daily set t5_date = %s where scode = %s and date = %s'
    update_daily_t5_kpi_sql = 'update daily set t5_kpi = %s where scode = %s and date = %s'

    #print(update_daily_tnprice_sql)
    
    with connection.cursor() as cursor:
        cursor.execute(select_daily_offtrade_sql)
        stock_lists = cursor.fetchall()
        stock_lists_length = len(stock_lists)        
        stock_list_index = 0
        while stock_list_index != stock_lists_length:
            n_days = 1
            #满足第二次有数据
            tn_index = 0
            #定义第n天价格是否存在，取返回值长度，1存在，0不存在
            tn_price_exists = 0
            #循环最大总数
            #loop_index = 0
            t_timearray = time.strptime(str(stock_lists[stock_list_index][2]), "%Y-%m-%d")
            t_timestamp = int(time.mktime(t_timearray))
            t_date = datetime.datetime.fromtimestamp(t_timestamp)        
            #t_date = stock_lists[stock_list_index][2]
            
            t_day = str(stock_lists[stock_list_index][2])            
            connection.commit()
            
            #tn_index:第n个交易日的价格，默认为2
            while (n_days != maxday and (tn_price_exists != 1 or tn_index != 5)):
                tn_day = get_strdate_after_n_tdays(t_date,n_days)
                tn_timearray = time.strptime(tn_day, "%Y-%m-%d")
                tn_timestamp = int(time.mktime(tn_timearray))
                tn_date = datetime.datetime.fromtimestamp(tn_timestamp) 
                today_date = datetime.datetime.now()
                
                if tn_date < today_date:
                    t_info = tushare.get_k_data(stock_lists[stock_list_index][0],start=str(t_day),end=str(t_day))['close'].values
                    #print(tn_day)
                    tn_info = tushare.get_k_data(stock_lists[stock_list_index][0],start=str(tn_day),end=str(tn_day))['close'].values   
                    
                    #通过list长度判断是否有值，即是否停牌
                    tn_price_exists = len(tn_info)
                    
                    print('调试:' + str(stock_lists[stock_list_index][0]) + ', ' + str(stock_lists[stock_list_index][2]) + '后第[' + str(n_days) + ']交易日' + tn_day + ', 是否停牌[' + str(tn_price_exists) + '],交易日计数[' + str(tn_index) + ']')
                    if (tn_price_exists == 1 and tn_index == 1):
                        t2_price = tn_info[0]
                        #输出第2日价格结果到数据库
                        cursor.execute(update_daily_t2price_sql,(str(t2_price), str(stock_lists[stock_list_index][0]), str(stock_lists[stock_list_index][2]))) 
                        cursor.execute(update_daily_t2date_sql,(tn_day, stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))
                        
                        #取数据库中t_price值
                        cursor.execute(select_daily_tprice_sql,(stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))
                        t_price_histdata = cursor.fetchone()[0]                    
                        t_price = t_info[0]
                        
                        #判断之前数据库记录是否除权，如为否记录到日志
                        if t_price_histdata != t_price:
                                output = open(savedlogs,'w',encoding='gbk')
                                output.write(str(stock_lists[stock_list_index][1]) + '(' + str(stock_lists[stock_list_index][0]) + '), ' 
                                             + str(stock_lists[stock_list_index][2]) + '\n')#输出写入csv文件
                        
                        cursor.execute(update_daily_tprice_sql,(str(t_price), stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))
                        
                        #计算第2日绩效
                        t2_kpi = ((t2_price-t_price)/t_price)*100                                       
                        #更新第2日的kpi到数据库
                        cursor.execute(update_daily_t2_kpi_sql, (str(t2_kpi), stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))
                        
                        #输出第2日绩效结果到屏幕
                        print(stock_lists[stock_list_index][1] + '(' + stock_lists[stock_list_index][0] + '), 初始交易日[' + str(stock_lists[stock_list_index][2])
                              + ']价格[' + str(t_price) + '], 第2日[' + str(tn_day) + ']的价格[' + str(t2_price) + '], 绩效为[' + str(t2_kpi) + '%]')
                        tn_index = tn_index + 1
                        
                    elif (tn_price_exists == 1 and tn_index == 4):
                        t5_price = tn_info[0]
                        #输出第5日价格结果到数据库
                        cursor.execute(update_daily_t5price_sql,(str(t5_price), str(stock_lists[stock_list_index][0]), str(stock_lists[stock_list_index][2]))) 
                        cursor.execute(update_daily_t5date_sql,(tn_day, stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))
                        cursor.execute('update daily set offdate = NULL where scode = %s and date = %s',(stock_lists[stock_list_index][0],stock_lists[stock_list_index][2]))
                        #计算第5日绩效
                        t5_kpi = ((t5_price-t_price)/t_price)*100
                        #更新对应的kpi
                        cursor.execute(update_daily_t5_kpi_sql, (str(t5_kpi), stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))
                        #更新offtrade = 0, 非停牌标记
                        cursor.execute(update_daily_offtrade_sql,('0', stock_lists[stock_list_index][0],stock_lists[stock_list_index][2]))
                        print(stock_lists[stock_list_index][1] + '(' + stock_lists[stock_list_index][0] + '), 初始交易日[' + str(stock_lists[stock_list_index][2])
                              + ']价格[' + str(t_price) + '], 第5日[' + str(tn_day) + ']的价格[' + str(t5_price) + '], 绩效为[' + str(t5_kpi) + '%]')
                        tn_index = tn_index + 1
                    elif tn_price_exists == 1:
                        tn_index = tn_index + 1
                    
                else:
                    #print('调试:交易日期大于今天，跳过...')
                    if n_days == maxday - 1:
                        print('截止今日，没有获取到T+2或T+5的交易日价格，置offtrade为1，offdate为今日日期')
                        cursor.execute(update_daily_offdate_sql,(today_date.strftime("%Y-%m-%d"),stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))                                 
                        #将读取交易日超过29天的设置offtrade=1，停牌标记
                        cursor.execute(update_daily_offtrade_sql,('1', stock_lists[stock_list_index][0],stock_lists[stock_list_index][2]))
    
                connection.commit()
                n_days = n_days + 1
            stock_list_index = stock_list_index + 1
    return None


update_tnprice(connection,60)
connection.close()
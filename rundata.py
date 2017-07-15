#coding=utf-8
#Author: yimzhu(yiming0330@gmail.com)
#Created: 20170603

import urllib.request,urllib.parse,json,os,time,datetime,http.cookiejar,string,os,sys,pymysql,re,tushare
#数据获取起始日期
startFileName = r'startDay.txt'
#存交易所原始数据
data_folder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/data'

#host = '192.168.100.100'
host = 'localhost'
username = 'root'
#password = ''
password = 'password'
db_name = 'dt'

# 打开数据库连接
connection = pymysql.connect(host=host,user=username,password=password,charset='utf8mb4',db=db_name)
output_folder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/output'

def get_opener(head):
    # deal with the Cookies
    cj = http.cookiejar.CookieJar()
    pro = urllib.request.HTTPCookieProcessor(cj)
    opener = urllib.request.build_opener(pro)
    header = []
    for key, value in head.items():
        elem = (key, value)
        header.append(elem)
    opener.addheaders = header
    return opener

def get_download_url(url):
    '''
    获取跳转后的真实下载链接
    :param url: 页面中的下载链接
    :return: 跳转后的真实下载链接
    '''
    req = urllib.request.Request(url)
    req.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko')
    response = urllib.request.urlopen(req)
    dlurl = response.geturl()     # 跳转后的真实下载链接
    return dlurl

def download_file(dlurl):
    '''
    从真实的下载链接下载文件
    :param dlurl: 真实的下载链接
    :return: 下载后的文件
    '''
    req = urllib.request.Request(dlurl)
    #req.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko')
    response = urllib.request.urlopen(req)
    return response.read()

def save_file(dlurl, dlfolder):
    '''
    把下载后的文件保存到下载目录
    :param dlurl: 真实的下载链接
    :param dlfolder: 下载目录
    :return: None
    '''
    os.chdir(dlfolder)              # 跳转到下载目录
    filename = dlurl.split('/')[-1] # 获取下载文件名
    dlfile = download_file(dlurl)
    with open(filename, 'wb') as f:
        f.write(dlfile)
        f.close()
    return None

def get_date_from_filename(strdate):
    '''
    %Y%m%d 格式转换为 %Y-%m-%d
    :param: string类型格式日期 %Y%m%d
    :return: string类型格式日期 %Y-%m-%d
    '''     
    gd_timearray = time.strptime(strdate, "%Y%m%d")
    gd_timestamp = int(time.mktime(gd_timearray))
    gd_day = datetime.datetime.fromtimestamp(gd_timestamp)
    return gd_day.strftime("%Y-%m-%d")

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
    :param: 日期型日期，第n天后
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

def get_filename(dir, fileList):
    '''
    获取文件夹下的文件列表
    :param s: 有营业部数据的行
    :return: None
    '''        
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

def remove_space_in_line(s):
    '''
    去除上证中数据存在营业部名称内可能存在的空格
    :param s: 有营业部数据的行
    :return: None
    '''     
    list=s.split()
    _no_space_in_agname_line = ''
    _counter = 1
    if len(list) > 3:
        length = len(list)-1
        for l in list: 
            if _counter <= length and _counter > 1:
                _no_space_in_agname_line = _no_space_in_agname_line + l
                _counter = int(_counter) + 1
            elif _counter == len(list):
                _no_space_in_agname_line = _no_space_in_agname_line + ' ' + l
                _counter = int(_counter) + 1
            elif _counter == 1:
                _no_space_in_agname_line = l + ' '
                _counter = int(_counter) + 1
    elif len(list) == 3:
        _no_space_in_agname_line = s
    return _no_space_in_agname_line 

def export_dailylhb(date,dlfolder,opfolder):
    '''
    读取抓取数据开始日期
    如果不存在该日期，从60日前开始读取
    如果存在从文件内日期开始读取
    读到今天
    :param dlurl: 真实的下载链接
    :param dlfolder: 下载目录
    :param date: 日期型日期
    '''
    header = {
        'Connection': 'Keep-Alive',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.111 Safari/537.36',
        'Accept-Encoding': 'gzip, deflate',
        'Host': '',
        'Referer' : ''
    }
    
    header['Host'] = 'query.sse.com.cn'
    header['Referer'] = 'http://www.sse.com.cn/disclosure/diclosure/public/'
    
    shUrl = 'http://query.sse.com.cn/infodisplay/showTradePublicFile.do?dateTx='#2015-09-28
    szUrl = ['http://www.szse.cn/szseWeb/common/szse/files/text/jy/jy',#150923.txt
             'http://www.szse.cn/szseWeb/common/szse/files/text/smeTxt/gk/sme_jy',#150708.txt
             'http://www.szse.cn/szseWeb/common/szse/files/text/nmTxt/gk/nm_jy']#150902.txt

    #设置异常标志位，如果上证没有数据，则深圳的数据抓取直接跳过
    onworkdata_flag = 0
    
    #给上证数据设置flag来区分买入卖出的写值
    direction_flag = 0
    #上证数据存入变量
    result = ''
    
    #抓取上证龙虎榜数据的地址
    shUrlwDate = shUrl + date.strftime("%Y-%m-%d")

    #存处理过的买入数据
    #opfolder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/output'    
    #上证抓取数据保存文件名
    savedDataCsv = opfolder + '/' + date.strftime("%Y%m%d") +'.csv'
    
    # 打开数据库连接
    #db = MySQLdb.connect("localhost","root","password","dt" )
    
    try:
        opener = get_opener(header)
        op = opener.open(shUrlwDate)
        data = op.read()
        data = data.decode()
        jsonData = json.loads(data)
    
        #证券名和代码
        security = ''
        
        if (jsonData['fileContents']!=''):
            for info in jsonData['fileContents']:
                if info.startswith('      证券代码: '):
                    result = result + '\n' + date.strftime("%Y-%m-%d") + ',' + info.split()[1]+','+info.split()[3]
                    security = info.split()[1]+','+info.split()[3]
                elif info.startswith('      买入营业部名称: '):
                    result = result + ',' + '买入'
                    direction_flag = 0
                elif info.startswith('      卖出营业部名称: '):
                    result = result + '\n' + date.strftime("%Y-%m-%d") + ',' + security + ',' + '卖出'
                    direction_flag = 1  
                elif '融资买入会员名称' in info and not '%' in info:
                    result = result + ',' + '融资买入'
                    direction_flag = 0
                elif '融资卖出会员名称' in info  and not '%' in info:
                    result = result + '\n' + date.strftime("%Y-%m-%d") + ',' + security + ',' + '融资卖出'
                    direction_flag = 1  
                elif info.startswith('  (1) '):
                    result = result + ',' + remove_space_in_line(info).split()[1] + ',' + remove_space_in_line(info).split()[2]
                elif info.startswith('  (2) '):
                    result = result + ',' + remove_space_in_line(info).split()[1] + ',' + remove_space_in_line(info).split()[2]
                elif info.startswith('  (3) '):
                    result = result + ',' + remove_space_in_line(info).split()[1] + ',' + remove_space_in_line(info).split()[2]
                elif info.startswith('  (4) '):
                    result = result + ',' + remove_space_in_line(info).split()[1] + ',' + remove_space_in_line(info).split()[2]
                elif info.startswith('  (5) '):                
                    result = result + ',' + remove_space_in_line(info).split()[1] + ',' + remove_space_in_line(info).split()[2]
        else:
            print("Json字符串为空！请检查原始数据！")
        #print(result.strip())
        output = open(savedDataCsv,'w',encoding='gbk')
        output.write(result.strip())#输出写入csv文件
    except:
        onworkdata_flag=1
        print(date.strftime("%Y-%m-%d")+'的上交所数据没有找到，请检查日期是否正确')
    finally:
        flag= 0 #重置flag
        
    #抓取深证，中小创交易龙虎榜数据
    #前缀，区分深圳主板，深圳中小板，深圳创业板
    prefix = ''
    #循环次数
    cycle=1
    
    #头信息
    header['Host'] = 'www.szse.cn'
    header['Referer'] = 'http://www.szse.cn'
    
    #数据输出
    result = ''
    
    for url in szUrl:
        if cycle==1:
            prefix = 'jy' #深圳主板
        elif cycle==2:
            prefix = 'sme_jy' #深圳中小板
        else:
            prefix = 'nm_jy'    #深圳创业板
        if onworkdata_flag == 0:
            szurl = url + date.strftime("%y%m%d")+'.txt'  
            dlurl = get_download_url(szurl)  # 真实下载链接
            #time.sleep(100)
            save_file(dlurl, dlfolder)     # 下载并保存文件    
            filename = dlfolder+'/'+prefix+date.strftime("%y%m%d")+'.txt'
            
        try:    
            with open(filename, 'rt', encoding='gbk') as f:
                #行数计
                linenum = 0
                #买卖方向标记
                direction_flag = 0
                #连续买入3日的数据不做统计，用此标记
                buythreeday_flag = 0
                #记录需要采集信息行数
                index = ''
                
                for line in f:
                    linenum = linenum+1
                    if '代码' in line and not '披露原因' in line and buythreeday_flag == 0:
                        if index == '':
                            index = str(linenum)
                        else:
                            index = index + ',' + str(linenum)
                    elif not '%' in line and '.' in line  and direction_flag == 0 and buythreeday_flag == 0:
                        index = index + ',' + str(linenum)
                    elif '买入金额最大的前' in line:
                        direction_flag = 0
                    elif '卖出金额最大的前' in line:
                        direction_flag = 1
                    elif '连续三个交易日内' in line:
                        buythreeday_flag = 1
                byTopList = index.split(',')
            
            with open(filename, 'rt', encoding='gbk') as f:
                linenum = 0
                for line in f:
                    linenum=linenum+1
                    if str(linenum) in byTopList:
                        if '代码' in line:
                            stringIndex=line.find('码') + 1                        
                            result = result + '\n' + date.strftime("%Y-%m-%d") + ',' +line[stringIndex:stringIndex+6] + ',' + line[:stringIndex-3] + ',' + '买入'                         
                        else:
                            #content = line.split()[0]
                            result = result + ',' + line.split()[0] + ',' + line.split()[1] 
            #print(result)               
            output.write(result)                                   
        except:
            print(date.strftime("%Y-%m-%d")+'的深交所数据没有找到，请检查日期是否正确')
        finally:
            direction_flag = 0
        cycle=cycle+1
    return None

def update_tnprice(connection, maxday, t5day):
    '''
    更新T+2和T+5绩效表
    :param: connection 
    :param: tnday 只能为 2 或 5
    :return: None
    '''        
    #存处理过的买入数据
    logs_folder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/logs'    
    #记录和原始数据库tprice不一致的日志
    saved_logs = logs_folder + '/' + 'updateprice.log'
    
    #select_daily_offtrade_sql = 'select distinct(scode),sname,date from daily where t2_date is null and offtrade = 1 and offdate is null／／／／ and offdate is not null order by date desc'
<<<<<<< HEAD
    select_daily_offtrade_sql = 'select distinct(scode),sname,date,t2_date,t2_price,t5_date from daily where offtrade is null and date <= %s '
=======
    select_daily_offtrade_sql = 'select distinct(scode),sname,date,t2_date,t2_price,t5_date from daily where offtrade is null and date < %s '
>>>>>>> 171e6cbf7522b2aec59a6b4396f5a7dab2f792e8
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
        cursor.execute(select_daily_offtrade_sql,(t5day))
        stock_lists = cursor.fetchall()
        stock_lists_length = len(stock_lists)        
        stock_list_index = 0
        while stock_list_index != stock_lists_length:
            n_days = 1
            #满足第二次有数据
            tn_index = 0
            #定义第n天价格是否存在，取返回值长度，1存在，0不存在
            tn_price_exists = 0
            
            t_timearray = time.strptime(str(stock_lists[stock_list_index][2]), "%Y-%m-%d")
            t_timestamp = int(time.mktime(t_timearray))
            t_date = datetime.datetime.fromtimestamp(t_timestamp) 
            t_day = str(stock_lists[stock_list_index][2])            
            
            #tn_index:第n个交易日的价格，默认为2
            while (n_days != maxday and (tn_price_exists != 1 or tn_index != 5)):
                tn_day = get_strdate_after_n_tdays(t_date,n_days)
                tn_timearray = time.strptime(tn_day, "%Y-%m-%d")
                tn_timestamp = int(time.mktime(tn_timearray))
                tn_date = datetime.datetime.fromtimestamp(tn_timestamp) 
                today_date = datetime.datetime.now()
                
                if tn_date < today_date:
                    t_info = tushare.get_k_data(stock_lists[stock_list_index][0],start=str(t_day),end=str(t_day))['close'].values
                    tn_info = tushare.get_k_data(stock_lists[stock_list_index][0],start=str(tn_day),end=str(tn_day))['close'].values   
                    
                    #通过list长度判断是否有值，即是否停牌
                    tn_price_exists = len(tn_info)
                    
                    #print('调试:' + str(stock_lists[stock_list_index][0]) + ', ' + str(stock_lists[stock_list_index][2]) + '后第[' + str(n_days) + ']交易日' + tn_day + ', 是否停牌[' + str(tn_price_exists) + '],交易日计数[' + str(tn_index) + ']')
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
                                output = open(saved_logs,'w',encoding='gbk')
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
<<<<<<< HEAD
                    connection.commit()
=======
>>>>>>> 171e6cbf7522b2aec59a6b4396f5a7dab2f792e8
                    
                else:
                    #print('调试:交易日期大于今天，跳过...')
                    if n_days == maxday - 1:
                        #取次日交易日价格，避免交易时段获取不到价格信息
                        td = get_strdate_before_n_tdays(datetime.datetime.now(),1)
                        day_info = tushare.get_k_data(stock_lists[stock_list_index][0],start=str(td),end=str(td))['close'].values
                        
                        if len(day_info) == 0:
                            print('截止今日，没有获取到T+2或T+5的交易日价格，置offtrade为1，offdate为今日日期')
                            cursor.execute(update_daily_offdate_sql,(today_date.strftime("%Y-%m-%d"),stock_lists[stock_list_index][0], stock_lists[stock_list_index][2]))                                 
                            #将读取交易日超过29天的设置offtrade=1，停牌标记
                            cursor.execute(update_daily_offtrade_sql,('1', stock_lists[stock_list_index][0],stock_lists[stock_list_index][2]))
                        else:
                            print('交易日还未满5日，留空隔日获取数据')
                        connection.commit()
                n_days = n_days + 1
            stock_list_index = stock_list_index + 1
    return None

def save_csv_to_mysql(connection,opfolder,starttimestamp):
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
            #the_day = datetime.datetime.fromtimestamp(fn_timestamp) 

<<<<<<< HEAD
            if fn_timestamp >= starttimestamp:              
=======
            if fn_timestamp > starttimestamp:              
>>>>>>> 171e6cbf7522b2aec59a6b4396f5a7dab2f792e8
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
    return None

'''################################主程序开始段####################################'''
<<<<<<< HEAD
end_day = change_strdate_to_date(get_strdate_before_n_tdays(datetime.datetime.now(),0),'%Y-%m-%d')
#print(end_day)
=======
end_day = change_strdate_to_date(get_strdate_before_n_tdays(datetime.datetime.now(),1),'%Y-%m-%d')

>>>>>>> 171e6cbf7522b2aec59a6b4396f5a7dab2f792e8
if os.path.exists(data_folder + '/' + startFileName):
    print('日期配置文件存在，开始读取')
    f=open(data_folder + '/' + startFileName,'rt')
    s = f.readline()
    f.close()
    if s!='':
        print('将从日期：'+s+' 开始读取')
        time_array = time.strptime(s, "%Y%m%d")
        time_stamp = int(time.mktime(time_array))
        start_day = datetime.datetime.fromtimestamp(time_stamp)
    else:
        print('日期配置文件为空，将从60日前日期开始读取')
        start_day = end_day - datetime.timedelta(days = 60)
else:
    print('日期配置文件不存在，将从60日前日期开始读取')
    start_day = end_day - datetime.timedelta(days = 60)

#设置从文件夹内读取文件的起始日期
from_day = start_day

#循环读取抓取的数据文件
while start_day.strftime("%Y-%m-%d")!=end_day.strftime("%Y-%m-%d"):
    print('正在读入' + start_day.strftime("%Y-%m-%d") + '的交易所数据')
<<<<<<< HEAD
    export_dailylhb(start_day,data_folder,output_folder)
=======
    #export_dailylhb(start_day,data_folder,output_folder)
>>>>>>> 171e6cbf7522b2aec59a6b4396f5a7dab2f792e8
    print(start_day.strftime("%Y-%m-%d")+'的交易所数据已经处理结束')
    start_day = start_day + datetime.timedelta(days = 1)

#最后更新日期为当前日期
<<<<<<< HEAD
updated_start_day = start_day# - datetime.timedelta(days = 1)
=======
updated_start_day = start_day - datetime.timedelta(days = 1)
>>>>>>> 171e6cbf7522b2aec59a6b4396f5a7dab2f792e8

fromday_timearray = time.strptime(from_day.strftime("%Y-%m-%d"), "%Y-%m-%d")
fromday_timestamp = int(time.mktime(fromday_timearray))

<<<<<<< HEAD
save_csv_to_mysql(connection,output_folder,fromday_timestamp)                    
update_tnprice(connection,6,get_strdate_before_n_tdays(datetime.datetime.now(),6))
=======
#save_csv_to_mysql(connection,output_folder,fromday_timestamp)                    
update_tnprice(connection,10,get_strdate_before_n_tdays(datetime.datetime.now(),5))
>>>>>>> 171e6cbf7522b2aec59a6b4396f5a7dab2f792e8

connection.close()

print('给文件设置最新日期')
f=open(data_folder + '/' + startFileName,'w')
f.write(updated_start_day.strftime("%Y%m%d"))
f.close()
print('操作结束。')

#coding=utf-8
#Author: yimzhu(yiming0330@gmail.com)
#Created: 20170603
#步骤1：从交易所获取龙虎榜信息，清洗数据写入csv文件

import urllib.request,urllib.parse,json,os,time,datetime,http.cookiejar,string,os,sys,pymysql,re,tushare
#数据获取起始日期
startFileName = r'startDay.txt'
#存交易所原始数据
dl_folder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/data'

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
    req.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko')
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

def export_dailylhb(date,dlfolder):
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
    opfolder = os.path.split(os.path.realpath(sys.argv[0]))[0] + '/output'    
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

'''################################主程序开始段####################################'''
end_day = change_strdate_to_date(get_strdate_before_n_tdays(datetime.datetime.now(),1),'%Y-%m-%d')

if os.path.exists(dl_folder + '/' + startFileName):
    print('日期配置文件存在，开始读取')
    f=open(dl_folder + '/' + startFileName,'rt')
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

#print('开始更新数据，从' + start_day +'到' +  end_day)

while start_day.strftime("%Y-%m-%d")!=end_day.strftime("%Y-%m-%d"):
    print('正在读入' + start_day.strftime("%Y-%m-%d") + '的交易所数据')
    export_dailylhb(start_day,dl_folder)
    print(start_day.strftime("%Y-%m-%d")+'的交易所数据已经处理结束')
    start_day = start_day + datetime.timedelta(days = 1)

#最后更新日期为当前日期
print('设置最新日期')
start_day = start_day - datetime.timedelta(days = 1)
f=open(dl_folder + '/' + startFileName,'w')
f.write(start_day.strftime("%Y%m%d"))
f.close()
print('读取完成')

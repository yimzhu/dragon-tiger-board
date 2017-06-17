import tushare
import pymysql
from sqlalchemy import create_engine
import time,datetime

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

def change_quarter_to_date(str):
    date = ''
    if str == 1:
        date = '0331'
    elif str == 2:
        date = '0630'
    elif str == 3:
        date = '0930'
    else:
        date = '1231'
    return date


def get_topten_holders(engine, scode, syear, squarter):
    '''
    获取前十大股东数据
    :param: sqlalchemy连接实例
    :param: scode 股票代码
    :param: syear 年
    :param: squarter 季
    :return: None 无
    '''      
    df = tushare.top10_holders(code=scode,year=syear,quarter=squarter,gdtype='1') #gdtype为1时表示其他流通股，0表示包含限售股
    holders_info = list(df)[1]
    holders_info['code'] = scode
    holders_info.to_sql(name='topholders',con=engine,if_exists='append')
    return None

def check_tp10holder_already_exists(connection, syear, squarter):
    '''
    获取前十大股东记录是否在数据库已经存在
    :param: pymysql连接实例
    :param: scode 股票代码
    :param: syear 年
    :param: squarter 季
    :return: string 1存在 0 不存在
    ''' 
    
    column_quarter = ''
    result = 0
    if squarter == 1:
        column_quarter =  syear + '-03-31'
    elif squarter == 2:
        column_quarter =  syear + '-06-30'
    elif squarter == 3:
        column_quarter =  syear + '-09-30'
    else:
        column_quarter =  syear + '-12-31'
    
    select_tpholders_sql = 'select count(1) from topholders where code = %s and quarter = %s'
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(select_tpholders_sql,(scode,column_quarter))
            #print(scode + ',' + column_quarter)
            result = cursor.fetchone()[0]
            #print(result)
            connection.commit() 
    except:
        connection.rollback()
    return result

host = 'localhost'
username = 'root'
password = 'password'
db_name = 'dt'
syear = '2017'
squarter = 1

# 打开数据库连接
engine = create_engine("mysql+pymysql://root:password@localhost/dt?charset=utf8",echo=True)
connection = pymysql.connect(host=host,user=username,password=password,charset='utf8mb4',db=db_name)

select_all_stocks_sql = 'select code,timeToMarket from stock where q_flag = 0'
#select_codes_sql = 'select count(1) from topholders where code = %s'
update_qflag = 'update stock set q_flag = 1 where code = %s'
record_exists = check_tp10holder_already_exists(connection, syear, squarter)

if record_exists == 0:
    with connection.cursor() as cursor:
        cursor.execute(select_all_stocks_sql)
        result = cursor.fetchall()
        line_index = 0
        while line_index != len(result) - 1:
            #q_date = syear+change_quarter_to_date(squarter-1)
            q_date = '20161231'
            if int(result[line_index][1]) < int(q_date):
                print('code:' + result[line_index][0])
                get_topten_holders(engine, result[line_index][0], syear, squarter)   
                cursor.execute(update_qflag,result[line_index][0])
                connection.commit()
            line_index = line_index + 1
connection.close()
'''
时间：2017年12月24日
作者：Jason
目标：爬取http://pm25.in上的天气信息
版本：3.1
功能：获取全国各个城市的天气信息，并保存到数据库 以时间创表，每个表中保存的全国的数据
'''
import requests
from bs4 import BeautifulSoup
import pymysql
import time

def get_data_time():
    url = 'http://pm25.in/abazhou'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36'}
    res = requests.get(url,timeout = 100, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    times = soup.find_all('div', class_="live_data_time")
    for i in times:
        data_time = 'T' + i.text[8:12] + i.text[13:15] + i.text[16:18] + i.text[19:21] + i.text[22:24]+ i.text[25:27]
    return data_time

def get_data():
    data_time = get_data_time()
    db = pymysql.connect("localhost", "root", "810810", "environment", charset="utf8")
    cursor = db.cursor()
    cursor.execute("drop table if exists %s"%data_time)
    sql = """create table %s(
                     city char(10),
                     AQI char(10),
                     PM25_1h char(10),
                     PM10_1h char(10),
                     CO_1h char(10),
                     NO2_1h char(10),
                     O3_1h char(10),
                     O3_8h char(10),
                     SO2_1h char(10))""" % data_time
    cursor.execute(sql)
    print("\n creat table successd ! \n")
    db.close()
    url = 'http://pm25.in/'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36'}
    res = requests.get(url,timeout = 100, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    target = soup.select('.all a')
    for city in target:        #获取城市url后缀
        time.sleep(1.5)
        adress = city['href']
        loca = city.text
        # print(loca)
        time.sleep(1.5)
        url = 'http://pm25.in' + str(adress)
        res = requests.get(url,timeout = 100, headers=headers)
        soup = BeautifulSoup(res.text,'html.parser')
        target = soup.find_all('div',class_="value")
        data = []
        for i in range(8):           #获取环境数据
            data.append(target[i].text.replace(" ", "").replace("\n", " "))
        db = pymysql.connect("localhost", "root", "810810", "environment", charset="utf8")
        cursor = db.cursor()
        sqls = """insert into %s(City,AQI,PM25_1h,PM10_1h,CO_1h,NO2_1h,O3_1h,O3_8h,SO2_1h)
                                 values('%s','%s','%s','%s','%s','%s','%s','%s','%s')""" % (data_time,loca,data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7])
        try:
            cursor.execute(sqls)
            db.commit()
            print(loca + '数据提交成功')
        except:
            db.rollback()
            print('\n Some Error happend ! \n')

        db.close()

if __name__ == '__main__':
    print('\n\n程序已启动等待获取数据\n\n')
    while 1:
        ls = time.localtime()
        time.sleep(3)
        if (ls[3] >= 1):
            if(ls[4] == 6):         #每个小时当分钟为5的时候开始爬取数据
                try:
                    print(ls)
                    print('\n\n准备开始提交数据')
                    get_data()
                    print('\n\n继续等待数据更新\n\n')
                except:
                    print('程序出现错误')
                    time.sleep(1000)
                    get_data()


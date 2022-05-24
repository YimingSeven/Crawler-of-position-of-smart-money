# -*- coding: utf-8 -*-
"""
Pycharm Editor: Mr.seven
This is a temporary script file.

此脚本是港交所结算中心爬取的结果

"""
from lxml import etree
import requests
import csv
import os
import pandas as pd
from datetime import datetime
from numpy import random
import time

# 导入所有日期及需要爬取的股票代码
last_date_everyweek = pd.read_excel('E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/input_data/每周行业拥挤度-用函数更新/2012年以来每周五日期.xlsx',
                                    encoding='gbk', index_col=0, parse_dates=True)
# last_date_everyweek = last_date_everyweek.loc['2021-04-22':]
# last_date_everyweek = last_date_everyweek.loc[['2021-06-18', '2021-06-25', '2021-07-09', '2021-12-31', '2022-03-11', '2022-03-18', '2022-04-01', '2022-04-29', '2022-05-06'
#                                                , '2022-05-13', '2022-05-20'], :]
# last_date_everyweek = last_date_everyweek.loc[('2021-06-18', '2021-06-25'), '星期几']
# print(last_date_everyweek)
# all_date_list_ = last_date_everyweek.index.tolist()[13:-15]
all_date_list_ = ['2022-04-29', '2022-05-06'
                  , '2022-05-13', '2022-05-20']
all_date_list_ = pd.to_datetime(all_date_list_)
print(all_date_list_)
# exit()
# 所有陆股通代码
stock_code_ = pd.read_excel('E:/国联证券课题/周报/转债市场周报/20220415周/20210422至220422陆股通交易名单.xlsx', index_col=0, encoding='gbk')
stock_code_list_ = stock_code_['港交所结算代码'].tolist()

# stock_Ashare_code_list_ = stock_code_['证券代码'].tolist()
file_path = 'E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/output_data/所有的北向持仓数据/'
temp_time_list = [1, 2, 3]

# 循环遍历每个周五的交易日
for each_date in all_date_list_:
    this_date_1 = datetime.strftime(each_date, '%Y/%m/%d')
    this_date_2 = datetime.strftime(each_date, '%Y%m%d')
    # this_date_2 = '20220522'
    user_agent_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
        # "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
    ]

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
               'Connection': 'close'}
    # headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36", 'Connection': 'close'}
    # headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36"}
    # url = "https://www.hkexnews.hk/sdw/search/searchsdw_c.aspx"
    url = "https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"
    #
    # 判断是否存在文件夹
    foldername = "E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/output_data/所有的北向持仓数据/" + this_date_2
    word_name = os.path.exists(foldername)
    # 判断文件是否存在：不存在创建
    if not word_name:
        os.makedirs(foldername)
    # 将文件夹绝对路径设置到当前日期
    os.chdir("E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/output_data/所有的北向持仓数据/" + this_date_2 + '/')

    # 每日更新当前文件夹中已经爬到的标的数据
    for root, dirs, files in os.walk(file_path + this_date_2):
        # 每个文件夹中已经爬取的股票代码
        temp_ = [iii[:-4] for iii in files]
        temp_ii = 0
    print(this_date_1)
    # print(this_date_2)

    # 再遍历每个正股的港交所代码
    for each_code in stock_code_list_:
        headers['User-Agent'] = random.choice(user_agent_list)
        stock_Ashare_code_ = stock_code_.loc[stock_code_['港交所结算代码'] == each_code].index[0]
        print(each_code)
        print(stock_Ashare_code_)
        # 判断A股代码是否在
        if stock_Ashare_code_ in temp_:
            print('之前已经爬过该只券了~')
            temp_ii += 1
            print('第几个：', temp_ii)
            continue

        datatime_ = {'__EVENTTARGET': 'btnSearch',
                     # '__EVENTTARGET': 'btnSearch',
                     '__EVENTARGUMENT': '',
                     # '__VIEWSTATE': '/wEPDwULLTIwNTMyMzMwMThkZHNjXATvSlyVIlPSDhuziMEZMG94',
                     '__VIEWSTATE': '/wEPDwUKMTY0ODYwNTA0OWRkM79k2SfZ+VkDy88JRhbk+XZIdM0=',
                     '__VIEWSTATEGENERATOR': '3B50BBBD',
                     'today': this_date_2,
                     'sortBy': 'shareholding',
                     'sortDirection': 'desc',
                     'alertMsg': '',
                     'txtShareholdingDate': this_date_1,
                     'txtStockCode': each_code,
                     'txtStockName': '',
                     'txtParticipantID': '',
                     'txtParticipantName': ''
                     }
        # 至少有3次链接
        iii_ = 0
        while iii_ < 5:
            try:
                repensoe = requests.post(url, headers=headers, data=datatime_, verify=False, timeout=15)
                break
            except requests.exceptions.RequestException:
                iii_ += 1
        # 如果循环了5次还没连接上 或者 还没找到相应的港股代码标的则，丢掉这只券，考虑进入下一只券
        if iii_ == 5:
            # filenme = stock_Ashare_code_ + ".csv"
            # # 判断是否存在文件夹
            # foldername = "E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/output_data/所有的北向持仓数据/" + this_date_2
            # word_name = os.path.exists(foldername)
            # # 判断文件是否存在：不存在创建
            # if not word_name:
            #     os.makedirs(foldername)
            # # os.chdir("E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/output_data/所有的北向持仓数据/" + this_date_2 + '/')
            continue

        text = repensoe.text
        html1 = etree.HTML(text)
        # print(text)

        div = html1.xpath("//div[@id='pnlResultNormal']//tbody/tr")
        # print(div)
        # exit()
        # 初始化单个券，爬取的数据结果
        data = []
        # print("--" * 2)
        valuetime = html1.xpath("//input[@id='txtShareholdingDate']/@value")
        # print(valuetime[0] + "有数据")

        for tb in div:
            # print(tb.xpath("./td[1]/div/text()"))
            try:
                participant_id = tb.xpath("./td[1]/div/text()")[1]
            except IndexError:
                participant_id = ''
            try:
                participant_name = tb.xpath("./td[2]/div/text()")[1]
            except IndexError:
                participant_name = ''
            try:
                participant_address = tb.xpath("./td[3]/div/text()")[1]
            except IndexError:
                participant_address = ''

            right = tb.xpath("./td[4]/div/text()")[1]
            percent = tb.xpath("./td[5]/div/text()")[1]
            datadic = {"日期": valuetime[0], "参与者编号": participant_id, "中央系统参与者名称": participant_name, "地址": participant_address, "持股量": right, "占比": percent}
            data.append(datadic)
            # print(data)
            # exit()
        filenme = stock_Ashare_code_ + ".csv"
        # print(filenme)
        csvhead = ["日期", "参与者编号", "中央系统参与者名称", "地址", "持股量", "占比"]

        # # 判断是否存在文件夹
        # foldername = "E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/output_data/所有的北向持仓数据/" + this_date_2
        # word_name = os.path.exists(foldername)
        # # 判断文件是否存在：不存在创建
        # if not word_name:
        #     os.makedirs(foldername)
        # os.chdir("E:/pyseven/GuolianSec_work_dir/eighth_Convertible_bond_sentiment/output_data/所有的北向持仓数据/" + this_date_2 + '/')

        with open(filenme, 'w', newline='') as fp:
            write = csv.DictWriter(fp, csvhead)
            write.writeheader()
            write.writerows(data)
        # time.sleep(4)
        time.sleep(random.choice(temp_time_list))
    #   exit()

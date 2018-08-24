import os
import sys
import time
import random
import logging
import requests
from setting import *
from mysqlStore import KNsql
from bs4 import BeautifulSoup as BS

# 配置基础log信息，级别为NOTSET
logging.basicConfig(level=logging.INFO,
                    filename=path+'del.log')

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

# StreamHandler,仅打印警告及以上级别
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(level=logging.WARN)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - line %(lineno)d - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# FileHandler
file_handler = logging.FileHandler(path+'szLog.log')
file_handler.setLevel(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - file %(pathname)s - line %(lineno)d - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

command = sys.argv

if 'del' in command:
    os.remove(path+'shHistory.txt')
    logger.warning('Delete'+path+'shHistory.txt')
    os.remove(path+'szHistory.txt')
    logger.warning('Delete'+path+'szHistory.txt')

db = KNsql()
# 用于判断新内容是否重复
now = set()
try:
    his = set()
    with open(path+'szHistory.txt', 'r')as His:
        for line in His.readlines():
            his.add(line.replace('\n', ''))
        cap = len(his)
        print(f'History \'s capability is {cap}.')
except FileNotFoundError:
    print('First crawling...')

sernos = [0, 1, 2]
tabs = ['tab1', 'tab2', 'tab3']
values = ['主板', '中小企业版', '创业板']

for serno, tab, value in zip(sernos, tabs, values):
    page = 0
    maxPage = 100
    if_break = False
    while page < maxPage:
        page += 1
        token = str(random.random())
        url = f'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=main_wxhj&TABKEY={tab}' \
              f'&PAGENO={page}&random={token}'
        headers = {
            'Accept': "application/json, text/javascript, */*; q=0.01",
            'Accept-Encoding': "gzip, deflate",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Type': "application/json",
            'Host': "www.szse.cn",
            'Referer': "http://www.szse.cn/disclosure/supervision/inquire/index.html",
            'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
            'X-Request-Type': "ajax",
            'X-Requested-With': "XMLHttpRequest",
            'Cache-Control': "no-cache",
        }

        if if_break:
            break

        for i in range(6):
            try:
                res = requests.get(url, headers)
                logger.info('深圳' + value + ', 第' + str(page) + '页, 状态码:' + str(res.status_code))
                if res.status_code == 200:
                    break
                else:
                    continue
            except Exception as e:
                error = str(e)
                logger.warning(f'{url} has retried {i} times.')
                continue
            finally:
                time.sleep(3+3*random.random())
        else:
            logger.fatal(error)
            logger.fatal('Too many retry count.')
            raise BaseException('Fatal error: too many retry count.\nPlease read error.txt')

        data = res.json()[serno]['data']
        maxPage = int(res.json()[serno]['metadata']['pagecount'])

        for dic in data:
            with open(path+'szHistory.txt', 'a')as history:
                para = ' | '.join((str(dic['gsdm']), dic['gsjc'], dic['fhrq'], dic['hjlb']))
                ck = dic['ck']
                content = BS(ck).a['encode-open']
                if content not in his:
                    if content not in now:
                        now.add(content)
                        db.SZinsert(dic)
                        history.write(content + '\n')
                        logger.info('Has new：'+para)
                else:
                    if_break = True
                    break

db.connection.close()

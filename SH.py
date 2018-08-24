import os
import re
import sys
import json
import time
import random
import logging
import requests
from setting import *
from mysqlStore import KNsql

# 配置基础log信息，级别为NOTSET
logging.basicConfig(level=logging.INFO,
                    filename=path+'del.log')

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

# StreamHandler,仅打印警告及以上级别
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - line %(lineno)d: %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# FileHandler
file_handler = logging.FileHandler(path+'shLog.log')
file_handler.setLevel(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - file %(pathname)s - line %(lineno)d: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

command = sys.argv

if 'del' in command:
    os.remove(path+'shHistory.txt')
    logger.warning('Delete'+path+'shHistory.txt')
    os.remove(path+'szHistory.txt')
    logger.warning('Delete'+path+'szHistory.txt')

# 用于判断新内容是否重复
now = set()
db = KNsql()
try:
    his = set()
    with open(path+'shHistory.txt', 'r')as His:
        for line in His.readlines():
            his.add(line.replace('\n', ''))
        cap = len(his)
        print(f'History \'s capability is {cap}.')
except FileNotFoundError:
    print('First crawling...')

page = 0
maxPage = 100
if_break = False
while page < maxPage:
    page += 1
    rtime = str(time.time() - 2).replace('.', '')[:13]
    url = f"http://query.sse.com.cn/commonSoaQuery.do?jsonCallBack=jsonpCallback86733&siteId=" \
          f"28&sqlId=BS_GGLL&extGGLX=&stockcode=&channelId=10743%2C10744%2C10012&extGGDL=&orde" \
          f"r=createTime%7Cdesc%2Cstockcode%7Casc&isPagination=true&pageHelp.pageSize=15&pageHe" \
          f"lp.pageNo={page}&pageHelp.beginPage={page}&pageHelp.cacheSize=1&_={rtime}"
    headers = {
        'Accept': "*/*",
        'Accept-Encoding': "gzip, deflate",
        'Accept-Language': "zh-CN,zh;q=0.9",
        'Connection': "keep-alive",
        'Host': "query.sse.com.cn",
        'Referer': "http://www.sse.com.cn/disclosure/credibility/supervision/inquiries/",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like "
                      "Gecko) Chrome/67.0.3396.99 Safari/537.36",
        'Cache-Control': "no-cache",
    }
    if if_break:
        break
    for i in range(5):
        try:
            res = requests.get(url, headers=headers)
            logger.info('上海, ' + '第' + str(page) + '页，状态码：' + str(res.status_code))
            if res.status_code == 200:
                break
            else:
                continue
        except Exception as e:
            error = str(e)
            logger.warning(f'{url} has retried {i} times.')
            continue
        finally:
            time.sleep(random.random()*3+3)
    else:
        logger.fatal(error)
        logger.fatal('Too many retry count.')
        raise BaseException('Fatal error: too many retry count.\nPlease read error.txt')

    res = '{' + re.findall(r'{(.*)}', res.text)[0] + '}'
    data = json.loads(res)['pageHelp']
    data_list = data['data']
    maxPage = data['pageCount']
    with open(path+'shHistory.txt', 'a')as history:
        for dic in data_list:
            # 日期
            cmsOpDate = dic['createTime']
            # 公司代码
            extSECURITY_CODE = str(dic['extSECURITY_CODE'])
            # 函件类型
            extWTFL = dic['extWTFL']
            # 公司简称
            extGSJC = dic['extGSJC']
            # url
            content = dic['docURL']

            para = ' | '.join((cmsOpDate, str(extSECURITY_CODE), extWTFL, extGSJC))

            if content not in his:
                if content not in now:
                    logger.info('Has new：' + para)
                    db.insert(dic)
                    now.add(content)
                    history.write(content+'\n')
            else:
                if_break = True
                break

db.connection.close()

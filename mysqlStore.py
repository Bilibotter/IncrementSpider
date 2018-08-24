# coding=utf-8
import pymysql
from setting import *
from bs4 import BeautifulSoup as BS


class KNsql(object):
    def __init__(self):
        self.connection = pymysql.connect(host=host,
                                     user=username,
                                     password=password,
                                     charset='utf8',
                                     db=db_name)

        self.create_table_sql = """
        CREATE TABLE EXG_INQ(
        id INT AUTO_INCREMENT PRIMARY KEY,
        stockcode VARCHAR(10) NOT NULL,
        company VARCHAR(10) NOT NULL ,
        ANN_DT datetime NOT NULL ,
        lettertype VARCHAR(100) NOT NULL,
        content VARCHAR(300) NOT NULL, 
        OP_DATE datetime NOT NULL DEFAULT NOW()
        )
        """

        self.insert_table_sql = """
        INSERT INTO EXG_INQ(stockcode,company,ANN_DT,lettertype,content)
         VALUES('%s','%s','%s','%s','%s')
        """

        self.sql = ""

        with self.connection.cursor() as cursor:
            # 建表格式:stockcode,company,data,lettertype
            try:
                cursor.execute(self.create_table_sql)
            except:
                pass

    def insert(self, dic):
        with self.connection.cursor() as cursor:
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

            try:
                self.sql = self.insert_table_sql % (extSECURITY_CODE, extGSJC, cmsOpDate, extWTFL, content)
                self.insert_table_sql % (extSECURITY_CODE, extGSJC, cmsOpDate, extWTFL, content)
                cursor.execute(self.sql.encode('utf-8'))
                self.connection.commit()
                para = ' | '.join((cmsOpDate, str(extSECURITY_CODE), extWTFL, extGSJC))
                print('Insert finished:'+para)
            except Exception as e:
                print(extSECURITY_CODE, extGSJC, cmsOpDate, extWTFL, content)
                print(self.sql)
                print(e)

    def SZinsert(self, dic):
        with self.connection.cursor() as cursor:
            # 日期
            cmsOpDate = dic['fhrq']
            # 公司代码
            extSECURITY_CODE = str(dic['gsdm'])
            # 函件类型
            extWTFL = dic['hjlb']
            # 公司简称
            extGSJC = dic['gsjc']
            # url
            ck = dic['ck']
            content = BS(ck).a['encode-open']

            try:
                self.sql = self.insert_table_sql % (extSECURITY_CODE, extGSJC, cmsOpDate, extWTFL, content)
                self.insert_table_sql % (extSECURITY_CODE, extGSJC, cmsOpDate, extWTFL, content)
                cursor.execute(self.sql.encode('utf-8'))
                self.connection.commit()
            except Exception as e:
                print(extSECURITY_CODE, extGSJC, cmsOpDate, extWTFL, content)
                print(self.sql)
                print(e)

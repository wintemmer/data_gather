from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, create_engine
import tushare as ts
from tqdm import tqdm
import pandas as pd
import datetime


class Data_macro:
    def __init__(self):
        self.pro = ts.pro_api(
            'ead39614aec755d1c4143fe53fceaf7b2bfc0c09b987b040e520f50d')
        self.engine = create_engine(
            'mysql+pymysql://root:@SUMMER1234huawei@localhost:3306/macro')

    def stock_update(self, start=-1, end=-1):
        if start != -1 and end != -1:
            self.start = start
            self.end = end
        else:
            self.start, self.end = self.get_start_end()
            if start != -1:
                self.start = start
            if end != -1:
                self.end = end

        # 利率
        self.write_shibor()
        self.write_shibor_quote()
        self.write_shibor_lpr()
        self.write_libor()
        self.write_hibor()

    # 开始和结束

    def get_start_end(self):
        sql = 'SELECT date FROM shibor WHERE date = (select max(date) from shibor)'
        df = pd.read_sql_query(sql, self.engine)
        lastday = df.iloc[0, 0]

        end = datetime.datetime.now().strftime("%Y%m%d")
        start = (datetime.datetime.strptime(lastday, '%Y%m%d') +
                 datetime.timedelta(days=1)).strftime("%Y%m%d")
        return start, end


    # 每日交易数据
    def write_shibor(self):
        df = self.pro.shibor(start_date=self.start, end_date=self.end)
        df.to_sql('shibor', self.engine, index=False,
                  if_exists='append', chunksize=5000)
    
    def write_shibor_quote(self):
        df = self.pro.shibor_quote(start_date=self.start, end_date=self.end)
        df.to_sql('shibor_quote', self.engine, index=False,
                  if_exists='append', chunksize=5000)

    def write_shibor_lpr(self):
        df = self.pro.shibor_lpr(start_date=self.start, end_date=self.end)
        df.to_sql('shibor_lpr', self.engine, index=False,
                  if_exists='append', chunksize=5000)

    def write_libor(self):
        df = self.pro.libor(start_date=self.start, end_date=self.end)
        df.to_sql('libor', self.engine, index=False,
                  if_exists='append', chunksize=5000)

    def write_hibor(self):
        df = self.pro.hibor(start_date=self.start, end_date=self.end)
        df.to_sql('hibor', self.engine, index=False,
                  if_exists='append', chunksize=5000)

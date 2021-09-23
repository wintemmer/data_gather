from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, create_engine
import tushare as ts
from tqdm import tqdm
import pandas as pd
import datetime

class Data_stock:
    def __init__(self):
        self.pro = ts.pro_api('ead39614aec755d1c4143fe53fceaf7b2bfc0c09b987b040e520f50d')
        self.engine = create_engine(
            'mysql+pymysql://root:@SUMMER1234huawei@localhost:3306/stocks')

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

        self.dates = self.get_dates(self.start, self.end)
        
        for date in tqdm(self.dates):
            # 基础数据
            self.write_daily(date)
            self.write_daily_basic(date)
            self.write_moneyflow(date)

            # 涨跌停
            self.write_stk_limit(date)
            self.write_limit_list(date)

            # 北上
            self.write_hsgt_moneyflow(date)
            self.write_hsgt_top10(date)
            self.write_hsgt_hold(date)

            # 复权
            self.write_adj_factor(date)

            # 停牌
            self.write_suspend(date)

            # 管理人
            self.write_stk_managers(date)

            # 融资融券
            self.write_margin(date)
            self.write_margin_detail(date)

            # 龙虎榜
            self.write_top_list(date)
            self.write_top_inst(date)

            # 回购
            self.write_repurchase(date)

            # 新股上市
            # self.write_new_share(date) # - 此数据还没有获取
    
    ###############################################################################################
    #####################################   stock update   ########################################
    ###############################################################################################

    # 开始和结束
    def get_start_end(self):
        sql = 'SELECT trade_date FROM daily WHERE trade_date = (select max(trade_date) from daily)'
        df = pd.read_sql_query(sql, self.engine)
        lastday = df.iloc[0, 0]

        end = datetime.datetime.now().strftime("%Y%m%d")
        start = (datetime.datetime.strptime(lastday, '%Y%m%d') +
                datetime.timedelta(days=1)).strftime("%Y%m%d")
        return start, end

    # 获得日期
    def get_dates(self, start, end):
        df = self.pro.trade_cal(exchange='', start_date=start, end_date=end)
        dates = df.cal_date
        return dates

    # 每日交易数据
    def write_daily(self, date):
        df = self.pro.daily(trade_date=date)
        df.to_sql('daily', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 每日指标
    def write_daily_basic(self, date):
        df = self.pro.daily_basic(ts_code='', trade_date=date)
        df.to_sql('daily_basic', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 交易数据
    def write_moneyflow(self, date):
        df = self.pro.moneyflow(trade_date=date)
        df.to_sql('moneyflow', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 涨跌幅
    def write_stk_limit(self, date):
        df = self.pro.stk_limit(trade_date=date)
        df.to_sql('stk_limit', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 涨跌停列表
    def write_limit_list(self, date):
        df = self.pro.limit_list(trade_date=date)
        df.to_sql('limit_list', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 沪深港通交易量
    def write_hsgt_moneyflow(self, date):
        df = self.pro.moneyflow_hsgt(start_date=date, end_date=date)
        df.to_sql('hsgt_moneyflow', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 沪深港通龙虎
    def write_hsgt_top10(self, date):
        df = self.pro.hsgt_top10(trade_date=date, market_type='')
        df.to_sql('hsgt_top10', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 沪深港通持股
    def write_hsgt_hold(self, date):
        df = self.pro.hk_hold(trade_date=date)
        df.to_sql('hsgt_hold', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 沪深港通复权
    def write_adj_factor(self, date):
        df = self.pro.adj_factor(ts_code='', trade_date=date)
        df.to_sql('adj_factor', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 沪深港通退市
    def write_suspend(self, date):
        df = self.pro.suspend(ts_code='', suspend_date=date,
                        resume_date='', fields='')
        df.to_sql('suspend', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 股票管理人
    def write_stk_managers(self, date):
        df = self.pro.stk_managers(ann_date=date)
        df.to_sql('stk_managers', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 新股上市
    def write_new_share(self, date):
        df = self.pro.new_share(start_date=date, end_date=date)
        df.to_sql('new_share', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 融资融券总量
    def write_margin(self, date):
        df = self.pro.margin(trade_date=date)
        df.to_sql('margin', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 融资融券细节
    def write_margin_detail(self, date):
        df = self.pro.margin_detail(trade_date=date)
        df.to_sql('margin_detail', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 龙虎榜
    def write_top_list(self, date):
        df = self.pro.top_list(trade_date=date)
        df.to_sql('top_list', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 机构龙虎榜
    def write_top_inst(self, date):
        df = self.pro.top_inst(trade_date=date)
        df.to_sql('top_inst', self.engine, index=False,
                if_exists='append', chunksize=5000)

    # 股票回购
    def write_repurchase(self, date):
        df = self.pro.repurchase(ann_date=date)
        df.to_sql('repurchase', self.engine, index=False,
                if_exists='append', chunksize=5000)

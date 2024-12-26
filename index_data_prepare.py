"""
基于米筐量化接口

指数数据储备及成分股波动率计算

000001.XSHG - 上证指数
000852.XSHG - 中证1000
000905.XSHG - 中证500
000688.XSHG - 科创50

"""
#%%
import pandas as pd
import rqsdk 
import rqdatac
rqdatac.init()
import datetime
from tqdm import tqdm

# %%
# 上证指数成分股列表 - 取最新
sh_index_code_list = pd.DataFrame(rqdatac.index_weights('000001.XSHG', date=None))
sh_index_code_list = sh_index_code_list.rename(columns={0: 'weight'})
#%%
#TODO 获取上证成分股历史数据
sh_index = rqdatac.get_price(sh_index_code_list.index, start_date='2014-07-01', end_date='2024-10-15', frequency='1d', fields=None, adjust_type='pre', skip_suspended =False, market='cn', expect_df=True)
sh_index = sh_index.reset_index()
sh_index.to_feather("sh_index.feather")
# %%
# TODO - 获取中证1000成分股历史数据
cn_1000_code_list = pd.DataFrame(rqdatac.index_weights('000852.XSHG', date=None))
cn_1000_code_list = cn_1000_code_list.rename(columns={0: 'weight'})
cn_1000 = rqdatac.get_price(cn_1000_code_list.index, start_date='2014-07-01', end_date='2024-10-15', frequency='1d', fields=None, adjust_type='pre', skip_suspended =False, market='cn', expect_df=True)
cn_1000 = cn_1000.reset_index()
cn_1000.to_feather("cn_1000.feather")
# %%
# TODO - 获取中证500成分股历史数据
cn_500_code_list = pd.DataFrame(rqdatac.index_weights('000905.XSHG', date=None))
cn_500_code_list = cn_500_code_list.rename(columns={0: 'weight'})
cn_500 = rqdatac.get_price(cn_500_code_list.index, start_date='2014-07-01', end_date='2024-10-15', frequency='1d', fields=None, adjust_type='pre', skip_suspended =False, market='cn', expect_df=True)
cn_500 = cn_500.reset_index()
cn_500.to_feather("cn_500.feather")
# %%
# TODO - 获取科创50成分股历史数据
tech_50_code_list = pd.DataFrame(rqdatac.index_weights('000688.XSHG', date=None))
tech_50_code_list = tech_50_code_list.rename(columns={0: 'weight'})
tech_50 = rqdatac.get_price(tech_50_code_list.index, start_date='2014-07-01', end_date='2024-10-15', frequency='1d', fields=None, adjust_type='pre', skip_suspended =False, market='cn', expect_df=True)
tech_50 = tech_50.reset_index()
tech_50.to_feather("tech_50.feather")

#%%
#TODO - 计算月度波动率 - 等权

sh_index = pd.read_feather("sh_index.feather")
sh_index['date'] = pd.to_datetime(sh_index['date'])
sh_index['month'] = sh_index['date'].dt.month
sh_index['year'] = sh_index['date'].dt.year
monthly_first_last = sh_index.groupby(['year', 'month','order_book_id']).agg(first_close=('close', 'first'),
                                                        last_close=('close', 'last')).reset_index()
monthly_first_last['monthly_return'] = (monthly_first_last['last_close'] - monthly_first_last['first_close']) / monthly_first_last['first_close']
monthly_volatility = monthly_first_last.groupby(['year', 'month'])['monthly_return'].std().reset_index(name='std_sh_index')
monthly_volatility.to_feather("monthly_volatility_sh_index.feather")
# %%

cn_1000 = pd.read_feather("cn_1000.feather")
cn_1000['date'] = pd.to_datetime(cn_1000['date'])
cn_1000['month'] = cn_1000['date'].dt.month
cn_1000['year'] = cn_1000['date'].dt.year
monthly_first_last = cn_1000.groupby(['year', 'month','order_book_id']).agg(first_close=('close', 'first'),
                                                        last_close=('close', 'last')).reset_index()
monthly_first_last['monthly_return'] = (monthly_first_last['last_close'] - monthly_first_last['first_close']) / monthly_first_last['first_close']
monthly_volatility = monthly_first_last.groupby(['year', 'month'])['monthly_return'].std().reset_index(name='std_cn_1000')
monthly_volatility.to_feather("monthly_volatility_cn_1000.feather")
# %%
cn_500 = pd.read_feather("cn_500.feather")
cn_500['date'] = pd.to_datetime(cn_500['date'])
cn_500['month'] = cn_500['date'].dt.month
cn_500['year'] = cn_500['date'].dt.year
monthly_first_last = cn_500.groupby(['year', 'month','order_book_id']).agg(first_close=('close', 'first'),
                                                        last_close=('close', 'last')).reset_index()
monthly_first_last['monthly_return'] = (monthly_first_last['last_close'] - monthly_first_last['first_close']) / monthly_first_last['first_close']
monthly_volatility = monthly_first_last.groupby(['year', 'month'])['monthly_return'].std().reset_index(name='std_cn_500')
monthly_volatility.to_feather("monthly_volatility_cn_500.feather")
# %%
tech_50 = pd.read_feather("tech_50.feather")
tech_50['date'] = pd.to_datetime(tech_50['date'])
tech_50['month'] = tech_50['date'].dt.month
tech_50['year'] = tech_50['date'].dt.year
monthly_first_last = tech_50.groupby(['year', 'month','order_book_id']).agg(first_close=('close', 'first'),
                                                        last_close=('close', 'last')).reset_index()
monthly_first_last['monthly_return'] = (monthly_first_last['last_close'] - monthly_first_last['first_close']) / monthly_first_last['first_close']
monthly_volatility = monthly_first_last.groupby(['year', 'month'])['monthly_return'].std().reset_index(name='std_tech_50')
monthly_volatility.to_feather("monthly_volatility_tech_50.feather")
# %%

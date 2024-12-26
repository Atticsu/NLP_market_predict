"""
参照《ChatGPT, Stock Market Predictability and Links to the Macroeconomy》
构建情绪指标
"""

#%%

import pandas as pd 
import numpy as np
from tqdm import tqdm

# 米筐数据 依赖米筐权限
# import rqsdk 
# import rqdatac 

# rqdatac.init()

tqdm.pandas()

def assign_sentiment(row): #情绪三列对应的值为内容情绪的置信度 取95%为对应情绪： -1：负面 0：中性 1：正面
    if row['Positive'] > 0.95:
        return 1
    elif row['Negative'] > 0.95:
        return -1
    else:
        return 0
    
#TODO - 对齐新闻影响的交易日: 如果发布在非交易日，则影响最近交易日；发布在交易日盘前，则影响当日；发布在交易日开盘后，则认为影响下一交易日
def get_influence_trade_date(row):
    date = row['date']
    time = row['time']
    trade_date = row['trade_date']
    next_trade_date = row['next_trade_date']
    if date < trade_date:
        return trade_date
    else:
        if time < pd.to_datetime('09:30:00').time():
            return trade_date
        else:
            return next_trade_date
        
def load_senti_ratio(file_path):
    bert_senti_df = pd.read_feather(file_path)
    tqdm.pandas(desc = 'assign_sentiment')
    bert_senti_df['sentiment'] = bert_senti_df.progress_apply(assign_sentiment, axis=1)

    # TODO - Load Trade Date Data


    # 基于米筐数据 得到交易日与非交易日对应表
    # trade_date = rqdatac.get_trading_dates(start_date='20100101', end_date='20241010')
    trade_date_data = pd.read_feather("trade_date.feather") #已保存到本地
    trade_date_data.rename(columns={0: "trade_date"}, inplace=True)
    trade_date_data['trade_date'] = pd.to_datetime(trade_date_data['trade_date'])
    trade_date_data = trade_date_data.sort_values('trade_date')
    trade_date_data['next_trade_date'] = trade_date_data['trade_date'].shift(-1)

    date_range = pd.DataFrame({'date': pd.date_range(start='2010-01-01', end= '2024-10-15')}) #留一些冗余出来
    trade_date_data = pd.merge_asof(date_range, trade_date_data, left_on='date', right_on='trade_date', direction='forward')


    bert_senti_df['create_time'] = pd.to_datetime(bert_senti_df['create_time'])
    bert_senti_df['date'] = bert_senti_df['create_time'].dt.date
    bert_senti_df['time'] = bert_senti_df['create_time'].dt.time
    bert_senti_df['date'] = pd.to_datetime(bert_senti_df['date'])
    bert_senti_df = bert_senti_df.merge(trade_date_data, on='date', how='left')
    
    #TODO - 对齐交易日
    tqdm.pandas(desc = 'Aligned trade date')
    bert_senti_df['influence_trade_date'] = bert_senti_df.progress_apply(get_influence_trade_date, axis=1)
    bert_senti_df['influence_trade_date'] = pd.to_datetime(bert_senti_df['influence_trade_date'])
    bert_senti_df['year'] = bert_senti_df['influence_trade_date'].dt.year
    bert_senti_df['month'] = bert_senti_df['influence_trade_date'].dt.month

    #TODO - 计算月度总量，pov量，neg量
    bert_senti_df['monthly_total_news_num'] = bert_senti_df.groupby(['year','month'])['rich_text'].transform('count')

    # 计算每月正向 sentiment 数量
    monthly_positive_counts = bert_senti_df[bert_senti_df['sentiment'] == 1].groupby(['year','month']).size()
    monthly_positive_counts = monthly_positive_counts.rename("monthly_positive_num").reset_index()

    # 计算每月负向 sentiment 数量
    monthly_negative_counts = bert_senti_df[bert_senti_df['sentiment'] == -1].groupby(['year','month']).size()
    monthly_negative_counts = monthly_negative_counts.rename("monthly_negative_num").reset_index()

    # merge回
    monthly_counts = pd.merge(monthly_positive_counts, monthly_negative_counts, on=['year','month'], how='outer')

    monthly_counts = monthly_counts.fillna(0).astype({'monthly_positive_num': int, 'monthly_negative_num': int})

    bert_senti_df = bert_senti_df.merge(monthly_counts, on=['year','month'], how='left')

    bert_senti_df['monthly_positive_num'] = bert_senti_df['monthly_positive_num'].fillna(0).astype(int)
    bert_senti_df['monthly_negative_num'] = bert_senti_df['monthly_negative_num'].fillna(0).astype(int)

    bert_senti_df['pov_ratio'] = bert_senti_df['monthly_positive_num'] / bert_senti_df['monthly_total_news_num']
    bert_senti_df['neg_ratio'] = bert_senti_df['monthly_negative_num'] / bert_senti_df['monthly_total_news_num']
    monthly_ratio_data = bert_senti_df[['year','month','pov_ratio','neg_ratio']].drop_duplicates(subset= ['year','month'])
    monthly_ratio_data = monthly_ratio_data.sort_values(['year','month'])
    return monthly_ratio_data
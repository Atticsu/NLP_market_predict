"""
检验情绪指标对于收益率/波动率的预测能力
"""

import pandas as pd 
import numpy as np
from tqdm import tqdm
import sentiment_analysis_score_constructor

def sentiment_analysis_score_main(file_path):
    monthly_ratio_data = sentiment_analysis_score_constructor.load_senti_ratio(file_path)
    #TODO - 读取指数月度收益率数据   1 3 6 9 12 window return
    index_data = pd.read_feather('index_data.feather')
    index_data = index_data.reset_index()
    index_data = index_data.rename(columns={'order_book_id': 'ticker', 'date': 'trade_date'})
    index_name_mapping = {
        "000001.XSHG": "上证指数",
        "000852.XSHG": "中证1000",
        "000905.XSHG": "中证500",
        "000688.XSHG": "科创50"
    }

    index_data["index_name"] = index_data["ticker"].map(index_name_mapping)
    index_data['trade_date'] = pd.to_datetime(index_data['trade_date'])
    index_data['month'] = index_data['trade_date'].dt.month
    index_data['year'] = index_data['trade_date'].dt.year
    monthly_first_last = index_data.groupby(['year', 'month','ticker']).agg(first_close=('close', 'first'),
                                                            last_close=('close', 'last')).reset_index()
    monthly_first_last['monthly_return'] = (monthly_first_last['last_close'] - monthly_first_last['first_close']) / monthly_first_last['first_close']

    monthly_first_last = monthly_first_last.sort_values(by = ['ticker','year','month'])

    monthly_index_return_data = pd.DataFrame()
    for index_data in tqdm(monthly_first_last['ticker'].unique()):
        monthly_first_last_tmp = monthly_first_last[monthly_first_last['ticker'] == index_data]
        monthly_first_last_tmp['3_month_return'] = (
            (monthly_first_last_tmp.groupby('ticker')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=3)  
            .apply(lambda x: x.prod() - 1)  
        )

        monthly_first_last_tmp['6_month_return'] = (
            (monthly_first_last_tmp.groupby('ticker')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=6)  
            .apply(lambda x: x.prod() - 1)  
        )

        monthly_first_last_tmp['9_month_return'] = (
            (monthly_first_last_tmp.groupby('ticker')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=9)  
            .apply(lambda x: x.prod() - 1)  
        )

        monthly_first_last_tmp['12_month_return'] = (
            (monthly_first_last_tmp.groupby('ticker')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=12)  
            .apply(lambda x: x.prod() - 1)  
        )

        monthly_index_return_data = pd.concat([monthly_index_return_data, monthly_first_last_tmp])


    index_name_mapping = {
        "000001.XSHG": "sh_index",
        "000852.XSHG": "cn_1000",
        "000905.XSHG": "cn_500",
        "000688.XSHG": "tech_50"
    }

    monthly_index_return_data["index_name"] = monthly_index_return_data["ticker"].map(index_name_mapping)

    # 读取波动率月度数据 - index_data_prepare.py给出
    std_data_list = ['sh_index','cn_1000','cn_500','tech_50']
    share_data = pd.DataFrame()
    for std_data in tqdm(std_data_list,desc = 'get share data'):
        share_data_tmp = pd.read_feather(f'{std_data}.feather')
        share_data_tmp['index_name'] = std_data
        share_data = pd.concat([share_data, share_data_tmp])


    share_data = share_data[['index_name','order_book_id', 'date','close']]
    share_data['date'] = pd.to_datetime(share_data['date'])
    share_data['month'] = share_data['date'].dt.month
    share_data['year'] = share_data['date'].dt.year

    monthly_share_first_last = share_data.groupby(['year', 'month','order_book_id','index_name']).agg(first_close=('close', 'first'),
                                                            last_close=('close', 'last')).reset_index()
    monthly_share_first_last['monthly_return'] = (monthly_share_first_last['last_close'] - monthly_share_first_last['first_close']) / monthly_share_first_last['first_close']
    monthly_share_first_last = monthly_share_first_last.sort_values(by = ['index_name','order_book_id','year','month'])

    all_ticker_monthly_return = pd.DataFrame()
    for ticker in tqdm(monthly_share_first_last['order_book_id'].unique(),desc = 'Calculate Monthly Lag Return Std and Sharpe'): #使用循环+loc 避免计算收益率的时候越界
        monthly_share_first_last_tmp = monthly_share_first_last[monthly_share_first_last['order_book_id'] == ticker]
        monthly_share_first_last_tmp['3_month_return'] = (
            (monthly_share_first_last_tmp.groupby('order_book_id')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=3)  
            .apply(lambda x: x.prod() - 1)  
        )

        monthly_share_first_last_tmp['6_month_return'] = (
            (monthly_share_first_last_tmp.groupby('order_book_id')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=6)  
            .apply(lambda x: x.prod() - 1)  
        )

        monthly_share_first_last_tmp['9_month_return'] = (
            (monthly_share_first_last_tmp.groupby('order_book_id')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=9)  
            .apply(lambda x: x.prod() - 1)  
        )

        monthly_share_first_last_tmp['12_month_return'] = (
            (monthly_share_first_last_tmp.groupby('order_book_id')['monthly_return']
            .transform(lambda x: x + 1))  
            .rolling(window=12)  
            .apply(lambda x: x.prod() - 1)  
        )
        all_ticker_monthly_return = pd.concat([all_ticker_monthly_return, monthly_share_first_last_tmp])

    volatility_data = all_ticker_monthly_return.groupby(['index_name','year', 'month'])['monthly_return'].std().reset_index(name='1_month_std')
    volatility_data['3_month_std'] = all_ticker_monthly_return.groupby(['index_name','year','month'])['3_month_return'].std().reset_index(name='3_month_std')['3_month_std']
    volatility_data['6_month_std'] = all_ticker_monthly_return.groupby(['index_name','year','month'])['6_month_return'].std().reset_index(name='6_month_std')['6_month_std']
    volatility_data['9_month_std'] = all_ticker_monthly_return.groupby(['index_name','year','month'])['9_month_return'].std().reset_index(name='9_month_std')['9_month_std']
    volatility_data['12_month_std'] = all_ticker_monthly_return.groupby(['index_name','year','month'])['12_month_return'].std().reset_index(name='12_month_std')['12_month_std']


    #TODO - lag_data  将未来数据与当前匹配到一行  用于回归
    # 

    analysis_return_data = monthly_index_return_data.merge(volatility_data, on=['index_name','year','month'])

    analysis_return_data['return_lag1'] = analysis_return_data.groupby('ticker')['monthly_return'].shift(-1)
    analysis_return_data['return_lag3'] = analysis_return_data.groupby('ticker')['3_month_return'].shift(-3)
    analysis_return_data['return_lag6'] = analysis_return_data.groupby('ticker')['6_month_return'].shift(-6)
    analysis_return_data['return_lag9'] = analysis_return_data.groupby('ticker')['9_month_return'].shift(-9)
    analysis_return_data['return_lag12'] = analysis_return_data.groupby('ticker')['12_month_return'].shift(-12)

    analysis_return_data['std_lag1'] = analysis_return_data.groupby('ticker')['1_month_std'].shift(-1)
    analysis_return_data['std_lag3'] = analysis_return_data.groupby('ticker')['3_month_std'].shift(-3)
    analysis_return_data['std_lag6'] = analysis_return_data.groupby('ticker')['6_month_std'].shift(-6)
    analysis_return_data['std_lag9'] = analysis_return_data.groupby('ticker')['9_month_std'].shift(-9)
    analysis_return_data['std_lag12'] = analysis_return_data.groupby('ticker')['12_month_std'].shift(-12)

    analysis_return_data = analysis_return_data.reindex(columns =['year','month','index_name','monthly_return','return_lag1','return_lag3','return_lag6','return_lag9','return_lag12','1_month_std','std_lag1','std_lag3','std_lag6','std_lag9','std_lag12'])

    #TODO - sharpe_ratio  计算sharpe 收益率
    analysis_return_data['monthly_sharpe'] = analysis_return_data['monthly_return'] / analysis_return_data['1_month_std']
    analysis_return_data['sharpe_lag1'] = analysis_return_data['return_lag1'] / analysis_return_data['std_lag1']
    analysis_return_data['sharpe_lag3'] = analysis_return_data['return_lag3'] / analysis_return_data['std_lag3']
    analysis_return_data['sharpe_lag6'] = analysis_return_data['return_lag6'] / analysis_return_data['std_lag6']
    analysis_return_data['sharpe_lag9'] = analysis_return_data['return_lag9'] / analysis_return_data['std_lag9']
    analysis_return_data['sharpe_lag12'] = analysis_return_data['return_lag12'] / analysis_return_data['std_lag12']

    analysis_return_data = analysis_return_data.merge(monthly_ratio_data, on=['year','month'])
    analysis_return_data = analysis_return_data.dropna()

    
    #TODO- 参考论文 进行回归
    import statsmodels.api as sm
    from statsmodels.formula.api import ols
    final_stat_info = pd.DataFrame()

    for x_choice in ['pov_ratio', 'neg_ratio']:
        results = []
        for index_name, group in tqdm(analysis_return_data.groupby('index_name'),desc = 'Regression'):
            # 构建回归模型
            # 对于每个return变量，分别以pov_ratio和neg_ratio作为自变量
            for return_lag in ['monthly_return','return_lag1', 'return_lag3', 'return_lag6', 'return_lag9', 'return_lag12']:
                # 因变量是return_lag，解释变量是pov_ratio和neg_ratio
                X = group[[x_choice]]
                X = sm.add_constant(X)  # 添加常数项
                y = group[return_lag]

                model = sm.OLS(y, X).fit()  # 回归分析
                results.append({
                    'index_name': index_name,
                    'y_choose': return_lag,
                    'coef': model.params[f'{x_choice}'],
                    't_value': model.tvalues[f'{x_choice}'],
                    'p_value': model.pvalues[f'{x_choice}'],
                    'adj_r_squared': model.rsquared_adj
                })

            for std_lag in ['1_month_std','std_lag1', 'std_lag3', 'std_lag6', 'std_lag9', 'std_lag12']:
                # 因变量是std_lag，解释变量是pov_ratio和neg_ratio
                X = group[[x_choice]]
                X = sm.add_constant(X)  # 添加常数项
                y = group[std_lag]

                model = sm.OLS(y, X).fit()  # 回归分析
                results.append({
                    'index_name': index_name,
                    'y_choose': std_lag,
                    'coef': model.params[f'{x_choice}'],
                    't_value': model.tvalues[f'{x_choice}'],
                    'p_value': model.pvalues[f'{x_choice}'],
                    'adj_r_squared': model.rsquared_adj
                })
            for sharpe_lag in ['monthly_sharpe','sharpe_lag1', 'sharpe_lag3', 'sharpe_lag6', 'sharpe_lag9', 'sharpe_lag12']:
                # 因变量是std_lag，解释变量是pov_ratio和neg_ratio
                X = group[[x_choice]]
                X = sm.add_constant(X)  # 添加常数项
                y = group[sharpe_lag]

                model = sm.OLS(y, X).fit()  # 回归分析
                results.append({
                    'index_name': index_name,
                    'y_choose': sharpe_lag,
                    'coef': model.params[f'{x_choice}'],
                    't_value': model.tvalues[f'{x_choice}'],
                    'p_value': model.pvalues[f'{x_choice}'],
                    'adj_r_squared': model.rsquared_adj
                })
        # 将结果整理成DataFrame
        results_df = pd.DataFrame(results)
        results_df['X'] = x_choice
        
        final_stat_info = pd.concat([final_stat_info, results_df])
    return final_stat_info


# NLP_based_marktet_risk_predict
通过NLP技术分析新闻和社交媒体中的市场情绪，预测证券市场的系统性风险

- xlcj_scrape.py: 新浪财经爬虫

- batch_save_xlcj_flash.py: 将爬取到的数据分batch存储，便于后续读取

- sentiment_base_finBERT.ipynb: 调用finBERT 获取情绪指标

- index_data_prepare.py: 调用米筐接口 获取指数数据

- sentiment_analysis_score_constructor.py: 构建情绪指标 pov_ratio和neg_ratio

- NLP_senti_forecast_main.py: 调用数据和最终实现回归的函数储存脚本

- main.ipynb: 调用NLP_senti_forecast_main获取回归后统计信息

- finbert_tone_chinese_stat_info.xlsx: 港科的中文模型得到指标的回归结果

- translate_en_finbert_origin_stat_info.xlsx: 翻译后调用原版模型回归结果
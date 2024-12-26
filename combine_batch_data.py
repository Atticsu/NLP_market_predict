"""
将两个模型读取后的data保存到本地
"""
#%%

import pandas as pd
import os
from tqdm import tqdm

#TODO - 读取中文模型数据
senti_file_path  = 'finbert_tone_chinese_output'
bert_senti_df = pd.DataFrame()
for file in tqdm(os.listdir(senti_file_path)):
    file_path = os.path.join(senti_file_path, file)
    tmp = pd.read_feather(file_path)
    bert_senti_df = pd.concat([bert_senti_df, tmp])
bert_senti_df = bert_senti_df[bert_senti_df['is_repeat'] ==0]
bert_senti_df = bert_senti_df.reindex(columns= ['create_time','rich_text','Neutral','Positive', 'Negative'])
bert_senti_df = bert_senti_df.sort_values(by = 'create_time', ascending=False)
bert_senti_df.to_feather('finbert_tone_chinese_output.feather')


#%%

#TODO - 读取翻译后英文模型数据
senti_file_path  = 'translate_en_finbert_origin_output'
bert_senti_df = pd.DataFrame()
for file in tqdm(os.listdir(senti_file_path)):
    file_path = os.path.join(senti_file_path, file)
    tmp = pd.read_feather(file_path)
    bert_senti_df = pd.concat([bert_senti_df, tmp])
bert_senti_df = bert_senti_df[bert_senti_df['is_repeat'] ==0]
bert_senti_df = bert_senti_df.reindex(columns= ['create_time','rich_text','translate_content','Neutral','Positive', 'Negative'])
bert_senti_df = bert_senti_df.sort_values(by = 'create_time', ascending=False)
bert_senti_df.to_feather('translate_en_finbert_origin_output.feather')

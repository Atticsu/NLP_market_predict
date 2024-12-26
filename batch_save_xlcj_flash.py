"""
将爬取的新浪财经数据批量存储

batch_size == 1000
"""

#%%
import pandas as pd
import os

file_path =  'xlcj_flash_important'
save_path = 'batch_save_xlcj_flash'
batch_size = 1000

count = 0
combined_df = pd.DataFrame()
for file in os.listdir(file_path):
    file_path = os.path.join(file_path, file)
    tmp_df = pd.read_excel(file_path)
    combined_df = pd.concat([combined_df, tmp_df])
    count += 1
    if count % batch_size == 0:
        combined_df.to_feather(os.path.join(save_path, f'page_{count // batch_size}.feather'))
        combined_df = pd.DataFrame()
        
# %%

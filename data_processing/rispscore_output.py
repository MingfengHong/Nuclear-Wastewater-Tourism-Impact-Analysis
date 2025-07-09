import pandas as pd
import jieba
import numpy as np
from openpyxl import load_workbook
from collections import Counter
from datetime import datetime
from pathlib import Path

# 读取原始数据
data_path = r"C:\Users\hongm\OneDrive\桌面\核污水事件分析\JPTour.xlsx"
df = pd.read_excel(data_path)

# 将时间列转换为日期格式，并显式指定日期格式
df['时间'] = pd.to_datetime(df['时间'], format='%Y年%m月%d日 %H:%M', errors='coerce')

# 读取词典数据
risk_word_path = r"C:\Users\hongm\OneDrive\桌面\核污水事件分析\RiskWord.xlsx"
risk_word_df = pd.read_excel(risk_word_path)
risk_words = risk_word_df['风险感知词汇'].dropna().tolist()

# 对文本进行分词
def tokenize_text(text):
    if isinstance(text, str):
        return list(jieba.cut(text))
    else:
        return []

# 填充缺失值为一个空字符串
df['txt'] = df['txt'].fillna('')

# 将文本列进行分词处理
df['tokens'] = df['txt'].apply(tokenize_text)

# 对每条微博进行词典匹配，并计算风险感知强度得分
def calculate_rpis(tokens, risk_words):
    word_count = Counter(tokens)
    matched_count = sum([word_count[word] for word in risk_words if word in word_count])
    total_words = len(tokens)

    if total_words == 0:
        return 0
    else:
        return matched_count / total_words

# 计算每条微博的风险感知强度得分 (RPIS)
df['RPIS'] = df['tokens'].apply(lambda tokens: calculate_rpis(tokens, risk_words))

# 将时间列转换为日期格式
df['时间'] = pd.to_datetime(df['时间'], errors='coerce')

# 提取年月
df['YearMonth'] = df['时间'].dt.to_period('M')

# 按月计算加权的风险感知强度得分
monthly_rpis = df.groupby('YearMonth')['RPIS'].agg(['mean', 'count'])

# 输出每个月的结果，如果某个月没有数据，则输出对应信息
for period, row in monthly_rpis.iterrows():
    if row['count'] == 0:
        print(f"{period} 没有数据")
    else:
        print(f"{period} 的加权平均RPIS：{row['mean']:.4f} (共 {row['count']} 条数据)")

# 保存结果到一个新的Excel文件
output_path = r"C:\Users\hongm\OneDrive\桌面\核污水事件分析\Monthly_RPIS.xlsx"
monthly_rpis.to_excel(output_path)

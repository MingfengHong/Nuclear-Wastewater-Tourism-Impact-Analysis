import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm
import warnings
warnings.filterwarnings("ignore")
from pmdarima import auto_arima
import matplotlib.font_manager as fm

# 1. 数据加载
tourism_df = pd.read_excel('JPVisit.xlsx')  # 包含 'time' 和 'visit'
rpist_df = pd.read_excel('Monthly_RPIS.xlsx')  # 包含 'YearMonth', 'mean', 'count'

# 2. 数据预处理
print("旅游数据预览:")
print(tourism_df.head())
print("\nRPIS数据预览:")
print(rpist_df.head())

# 2.1. 转换'time'列为datetime格式（已经是datetime类型，无需转换）
# 直接重命名'time'为'Date'
tourism_df.rename(columns={'time': 'Date'}, inplace=True)

# 2.2. 转换'YearMonth'列为datetime格式
rpist_df['Date'] = pd.to_datetime(rpist_df['YearMonth'], format='%Y/%m/%d %H:%M:%S')

# 2.3. 重命名列以便合并
rpist_df.rename(columns={'mean': 'RPIS_mean'}, inplace=True)

# 2.4. 选择需要的列
rpist_monthly = rpist_df[['Date', 'RPIS_mean']]

# 2.5. 查看转换后的数据
print("\n转换后的旅游数据预览:")
print(tourism_df.head())
print("\n转换后的RPIS数据预览:")
print(rpist_monthly.head())

# 3. 合并旅游数据和RPIS数据，按月份对齐
merged_df = pd.merge(
    tourism_df,
    rpist_monthly,
    on='Date',
    how='inner'
)

print("\n合并后的数据预览:")
print(merged_df.head())
print(merged_df.tail())

# 4. 将 'Date' 列设置为索引，以便后续时间序列切片
merged_df.set_index('Date', inplace=True)

# 5. 定义事件日期和窗口
event_date = pd.to_datetime('2023-04-01')
pre_event_window = 3  # 事件前3个月
post_event_window = 3  # 事件后3个月

pre_event_start = event_date - pd.DateOffset(months=pre_event_window)
pre_event_end = event_date - pd.DateOffset(months=1)

post_event_start = event_date
post_event_end = event_date + pd.DateOffset(months=post_event_window-1)

print(f"\n事件前窗口: {pre_event_start.strftime('%Y-%m')} 到 {pre_event_end.strftime('%Y-%m')}")
print(f"事件后窗口: {post_event_start.strftime('%Y-%m')} 到 {post_event_end.strftime('%Y-%m')}")

# 6. 数据划分
pre_event_data = merged_df.loc[pre_event_start:pre_event_end].copy()
post_event_data = merged_df.loc[post_event_start:post_event_end].copy()

print("\n事件前窗口数据预览:")
print(pre_event_data.head())
print("\n事件后窗口数据预览:")
print(post_event_data.head())

# 7. 建立基准模型（ARIMA）
model = auto_arima(pre_event_data['visit'], seasonal=False, stepwise=True, suppress_warnings=True)
print("\nARIMA模型摘要:")
print(model.summary())

forecast, confint = model.predict(n_periods=post_event_window, return_conf_int=True)
forecast_index = post_event_data.index
forecast_series = pd.Series(forecast, index=forecast_index, name='Predicted_Tourist_Count')

comparison_df = post_event_data.copy()
comparison_df = comparison_df.join(forecast_series)

comparison_df['Abnormal_Change'] = comparison_df['visit'] - comparison_df['Predicted_Tourist_Count']
comparison_df['Cumulative_Abnormal_Change'] = comparison_df['Abnormal_Change'].cumsum()

print("\n比较数据预览:")
print(comparison_df)

# 8. 配置 Matplotlib 显示中文
# 设置字体路径，这里以微软雅黑为例
font_path = 'C:\\Windows\\Fonts\\msyh.ttc'  # 微软雅黑字体路径
font_prop = fm.FontProperties(fname=font_path)

# 设置全局字体
plt.rcParams['font.family'] = ['Microsoft YaHei']  # 指定中文字体
plt.rcParams['axes.unicode_minus'] = False        # 解决负号 '-' 显示为方块的问题

# 9. 可视化结果
plt.figure(figsize=(14, 12))

# 实际与预测旅游人数
plt.subplot(3,1,1)
plt.plot(pre_event_data.index, pre_event_data['visit'], label='实际旅游人数 (事件前)', color='blue')
plt.plot(post_event_data.index, post_event_data['visit'], label='实际旅游人数 (事件后)', color='blue', linestyle='-')
plt.plot(forecast_series.index, forecast_series, label='预测旅游人数', color='red', linestyle='--')
plt.axvline(event_date, color='green', linestyle='--', label='事件日期')
plt.legend(prop=font_prop)
plt.title('实际 vs 预测旅游人数', fontproperties=font_prop)
plt.xlabel('日期', fontproperties=font_prop)
plt.ylabel('旅游人数', fontproperties=font_prop)

# 异常变化
plt.subplot(3,1,2)
sns.barplot(x=comparison_df.index.strftime('%Y-%m'), y='Abnormal_Change', data=comparison_df, palette='viridis')
plt.axhline(0, color='black', linewidth=0.8)
plt.axvline(event_date.strftime('%Y-%m'), color='green', linestyle='--')
plt.title('异常变化 (实际 - 预测)', fontproperties=font_prop)
plt.xlabel('月份', fontproperties=font_prop)
plt.ylabel('异常变化', fontproperties=font_prop)
plt.xticks(rotation=45, fontproperties=font_prop)

# 累计异常变化
plt.subplot(3,1,3)
plt.plot(comparison_df.index.strftime('%Y-%m'), comparison_df['Cumulative_Abnormal_Change'], marker='o', color='purple')
plt.axhline(0, color='black', linewidth=0.8)
plt.axvline(event_date.strftime('%Y-%m'), color='green', linestyle='--')
plt.title('累计异常变化', fontproperties=font_prop)
plt.xlabel('月份', fontproperties=font_prop)
plt.ylabel('累计异常变化', fontproperties=font_prop)
plt.xticks(rotation=45, fontproperties=font_prop)

plt.tight_layout()
plt.show()

# 10. 回归分析：RPIS对旅游人数的影响
regression_df = comparison_df.copy()
regression_df['RPIS'] = regression_df['RPIS_mean']

print("\n回归分析数据预览:")
print(regression_df[['visit', 'Predicted_Tourist_Count', 'Abnormal_Change', 'Cumulative_Abnormal_Change', 'RPIS']])

X = regression_df[['RPIS']]
X = sm.add_constant(X)  # 添加截距
y = regression_df['Abnormal_Change']

model_reg = OLS(y, X).fit()

print("\n回归分析结果:")
print(model_reg.summary())

# 11. 相关性分析
correlation = regression_df['RPIS'].corr(regression_df['Abnormal_Change'])
print(f"\nRPIS与异常变化的相关系数: {correlation:.4f}")

# 可视化散点图
plt.figure(figsize=(8,6))
sns.scatterplot(x='RPIS', y='Abnormal_Change', data=regression_df)
plt.title('RPIS与异常变化的关系', fontproperties=font_prop)
plt.xlabel('风险感知强度得分 (RPIS)', fontproperties=font_prop)
plt.ylabel('异常变化 (Abnormal Change)', fontproperties=font_prop)
plt.show()

import pandas as pd
import re
import os

# 读取 Excel 文件（注意文件路径中使用原始字符串，避免转义问题）
file_path = r"C:\Users\hongm\OneDrive\桌面\deepseek爬取\热门\output.xlsx"
df = pd.read_excel(file_path)

# 定义函数，检查时间字符串是否包含年份，如果不包含则在前面添加默认年份“2025年”
def add_year(time_str):
    # 如果数据为空，则直接返回原值
    if pd.isnull(time_str):
        return time_str
    # 将数据转换为字符串（以防数据类型不是字符串）
    s = str(time_str)
    # 使用正则表达式检查是否包含四位数字后跟“年”
    if re.search(r'\d{4}年', s) is None:
        return "2025年" + s
    else:
        return s

# 对“时间”这一列中的每个单元格应用该函数
df["time"] = df["time"].apply(add_year)

# 生成新的文件路径，新文件名为 output_modified.xlsx，保存在原目录下
dir_path = os.path.dirname(file_path)
new_file_path = os.path.join(dir_path, "output_modified.xlsx")

# 保存修改后的数据到新的 Excel 文件（不保存行索引）
df.to_excel(new_file_path, index=False)

print(f"修改后的文件已保存至：{new_file_path}")

from datetime import datetime, timedelta

# 设置起始日期
start_date = datetime(2025, 1, 1)

# 设置结束日期（到2025年2月10日）
end_date = datetime(2025, 2, 10)

# 基础网址模板
base_url = "https://s.weibo.com/weibo?q=deepseek&xsort=hot&suball=1&timescope=custom%3A{start_date}%3A{end_date}&Refer=g"

# 生成网址并输出
current_date = start_date
while current_date <= end_date:
    # 计算当前日期和下一天的日期
    next_day = current_date + timedelta(days=1)

    # 格式化日期为字符串 (yyyy-mm-dd-0)
    start_date_str = current_date.strftime('%Y%m%d-0')
    end_date_str = next_day.strftime('%Y%m%d-0')

    # 生成 URL
    url = base_url.format(start_date=start_date_str, end_date=end_date_str)
    print(url)

    # 更新当前日期
    current_date = next_day

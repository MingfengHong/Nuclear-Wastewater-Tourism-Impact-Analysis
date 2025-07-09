from base_setting import *
user_name = topic


f = open('./{}一级评论.json'.format(user_name), 'r', encoding='utf-8')
content = f.read()
first_class_pinglun = json.loads(content)


first_class_pinglun_df = list(first_class_pinglun.values())
first_class_pinglun_df_new = []
for i in first_class_pinglun_df:
    for j in i:
        first_class_pinglun_df_new.append(j)

first_class_pinglun_df = pd.DataFrame(first_class_pinglun_df_new)

second_id = first_class_pinglun_df[first_class_pinglun_df['total_number']>0].reset_index(drop=True)
second_id['total_number'].sum()
print('总共{}条一级评论, 有二级评论的一级评论数量{}'.format(first_class_pinglun_df.shape[0], second_id.shape[0]))

try:
    f = open('./{}二级评论.json'.format(user_name), 'r', encoding='utf-8')
    content = f.read()
    second_class_pinglun = json.loads(content)
    finish_mid = list(second_class_pinglun.keys())
except:
    second_class_pinglun = {}
    finish_mid = []


mid_index = 4
for mid_index in tqdm(range(second_id.shape[0])):
    mid_second_class_pinglun = []
    mid = second_id.iloc[mid_index, 1]
    if str(mid) in finish_mid:
        continue
    uid = second_id.loc[mid_index, 'user']['id']
    params = {
        'is_reload': '1',
        'id': str(mid),
        'is_show_bulletin': '2',
        'is_mix': '1',
        'fetch_level': '1',
        'max_id': '0',
        'count': '20',
        'uid': str(uid),
        'locale': 'zh-CN',
    }
    response = requests.get('https://weibo.com/ajax/statuses/buildComments', params=params,  headers=headers, timeout=20)
    rs = response.json()
    mid_second_class_pinglun = mid_second_class_pinglun + rs['data']
    max_id = rs['max_id']
    while max_id != 0 and len(rs['data']) != 0:
        # print(mid, mid_index)
        params.update(
            {'flow': '0',
            'max_id': max_id,
            }
        )
        response = requests.get('https://weibo.com/ajax/statuses/buildComments', params=params,
                                headers=headers, timeout=20)
        rs = response.json()
        mid_second_class_pinglun = mid_second_class_pinglun + rs['data']
        max_id = rs['max_id']
    second_class_pinglun[str(mid)] = mid_second_class_pinglun
    if mid_index % 100 == 0 and mid_index != 0:
        data_xiangqing = json.dumps(second_class_pinglun, ensure_ascii=False, indent=4)
        # 将 json 数据写入文件
        with open("{}二级评论.json".format(user_name), "w", encoding='utf-8') as file:
            file.write(data_xiangqing)


data_xiangqing = json.dumps(second_class_pinglun, ensure_ascii=False, indent=4)
# 将 json 数据写入文件
with open("{}二级评论.json".format(user_name), "w", encoding='utf-8') as file:
    file.write(data_xiangqing)
print('finish')


print('已采集二级评论的一级评论数量{}'.format(second_id.shape[0]))

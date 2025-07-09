from base_setting import *

#
f = open("{}详情.json".format(topic), 'r', encoding='utf-8')
content = f.read()
xiangqing_info = json.loads(content)

# 得到规范的博文信息
format_data_bowen = []
for bowen_name, bowen_info in xiangqing_info.items():
    try:
        zhengwen = bowen_info[0].get('text_raw', '')
        bowen_ip = bowen_info[0].get('region_name', '')
        username = bowen_info[0]['user']['screen_name']
        userid = bowen_info[0]['user']['id']
        mid = bowen_info[0]['id']
        dianzan = bowen_info[0]['attitudes_count']
        pinglun = bowen_info[0]['comments_count']
        zhuanfa = bowen_info[0]['reposts_count']
        date = bowen_info[0]['created_at']
        pic_num = bowen_info[0]['pic_num']
        try:
            video_watch = bowen_info[0]['page_info']['media_info']['online_users_number']
            video_id = bowen_info[0]['page_info']['media_info']['media_id']
        except Exception as e:
            video_watch = ''
            video_id = ''
        try:
            region_name = bowen_info[0]['region_name']
        except Exception as e:
            region_name = ''
        GMT_FORMAT = '%a %b %d %H:%M:%S +0800 %Y'
        date = str(datetime.strptime(date, GMT_FORMAT))
        try:
            longtext = bowen_info[1]['data'].get('longTextContent', '')
        except Exception as e:
            longtext = ''
        format_data_bowen.append([username, userid, mid, date, dianzan, pinglun, zhuanfa, zhengwen, longtext, region_name, pic_num, video_watch, video_id])
    except Exception as e:
        print(e)

format_data_bowen = pd.DataFrame(format_data_bowen,
                                 columns=['username', 'userid', 'mid', 'date', 'dianzan', 'pinglun', 'zhuanfa',
                                          'zhengwen', 'longtext', 'region_name', 'pic_num', 'video_watch', 'video_id'])

def key_word_fliter(x, key_word):
    words_content = x['zhengwen'] + x['longtext']
    words_content = words_content.lower()
    has_key = []
    for key_i in key_word:
        key_i = key_i.lower()
        if key_i in words_content:
            has_key.append(key_i)
    if len(has_key) == 0:
        return ''
    else:
        return ','.join(has_key)
format_data_bowen['flag_word'] = format_data_bowen.apply(lambda funp: key_word_fliter(funp, key_word), axis=1)
format_data_bowen.to_excel('规范的博文信息_{}.xlsx'.format(topic), index=False)
format_data_bowen['mid'].nunique()
has_pinglun_data = format_data_bowen[format_data_bowen['pinglun']>0].reset_index(drop=True)
# has_pinglun_data.to_excel('规范的博文信息_{}.xlsx'.format(topic), index=False)
print('博文总数{}, 有评论的博文数量{}'.format(format_data_bowen.shape[0], has_pinglun_data.shape[0]))

'''获取一级评论信息'''
user_name = topic
try:
    f = open('./{}一级评论.json'.format(user_name), 'r', encoding='utf-8')
    content = f.read()
    first_class_pinglun = json.loads(content)
    finish_mid = list(first_class_pinglun.keys())
    len(set(finish_mid))
except:
    first_class_pinglun = {}
    finish_mid = []


for mid_index in tqdm(range(has_pinglun_data.shape[0])):
    mid_first_class_pinglun = []
    mid = has_pinglun_data.iloc[mid_index, 2]
    if str(mid) in finish_mid:
        continue
    uid = has_pinglun_data.iloc[mid_index, 1]
    params = {
        'flow': '0',
        'is_reload': '1',
        'id': str(mid),
        'is_show_bulletin': '2',
        'is_mix': '0',
        'count': '10',
        'uid': str(uid),
        'fetch_level': '0',
        'locale': 'zh-CN',
    }
    response = requests.get('https://weibo.com/ajax/statuses/buildComments', params=params,  headers=headers, timeout=20)
    rs = response.json()
    mid_first_class_pinglun = mid_first_class_pinglun + rs['data']
    max_id = rs['max_id']
    while max_id != 0 and len(rs['data']) != 0:
        params.update(
            {'flow': '0',
            'max_id': max_id,
            'count': '20',}
        )
        response = requests.get('https://weibo.com/ajax/statuses/buildComments', params=params,
                                headers=headers, timeout=20)
        rs = response.json()
        mid_first_class_pinglun = mid_first_class_pinglun + rs['data']
        max_id = rs['max_id']
    first_class_pinglun[str(mid)] = mid_first_class_pinglun
    if mid_index % 100 == 0 and mid_index != 0:
        data_xiangqing = json.dumps(first_class_pinglun, ensure_ascii=False, indent=4)
        # 将 json 数据写入文件
        with open("{}一级评论.json".format(user_name), "w", encoding='utf-8') as file:
            file.write(data_xiangqing)




data_xiangqing = json.dumps(first_class_pinglun, ensure_ascii=False, indent=4)
# 将 json 数据写入文件
with open("{}一级评论.json".format(user_name), "w", encoding='utf-8') as file:
    file.write(data_xiangqing)

print('总计有{}条博文有评论,已采集{}条博文的评论'.format(has_pinglun_data.shape[0], len(first_class_pinglun.keys())))
print('finish')



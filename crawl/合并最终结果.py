import pandas as pd
import warnings
warnings.filterwarnings("ignore")

from base_setting import *
user_name = topic

data_base = pd.read_csv('{}_bowen_info.csv'.format(user_name), encoding='gb18030')

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
format_data_bowen['url'] = format_data_bowen.apply(lambda row: 'https://weibo.com/{}/{}'.format(row['userid'], mid_to_url(row['mid'])), axis=1)

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


f = open('./{}一级评论.json'.format(user_name), 'r', encoding='utf-8')
content = f.read()
first_class_pinglun = json.loads(content)

first_class_pinglun_df = list(first_class_pinglun.values())
first_class_pinglun_df_new = []
for i in first_class_pinglun_df:
    for j in i:
        first_class_pinglun_df_new.append(j)

first_class_pinglun_df = pd.DataFrame(first_class_pinglun_df_new)




f = open('./{}二级评论.json'.format(user_name), 'r', encoding='utf-8')
content = f.read()
second_class_pinglun = json.loads(content)



second_class_pinglun_df = list(second_class_pinglun.values())
second_class_pinglun_df_new = []
for i in second_class_pinglun_df:
    for j in i:
        second_class_pinglun_df_new.append(j)

second_class_pinglun_df = pd.DataFrame(second_class_pinglun_df_new)

data_base = data_base.drop_duplicates('mid').reset_index(drop=True)
format_data_bowen = format_data_bowen.drop_duplicates('mid').reset_index(drop=True)
first_class_pinglun_df = first_class_pinglun_df.drop_duplicates('id').reset_index(drop=True)
second_class_pinglun_df = second_class_pinglun_df.drop_duplicates('id').reset_index(drop=True)
###############################################################################各板块采集结果一览
print('博文id数量{}，获取到的博文详情数量{}，有{}条博文有评论, 获取到的一级评论的博文数量{}，获取到的一级评论量{}，有{}条一级评论有二级评论，获取到二级评论的一级评论数量{}，获取到的二级评论量{}'.format(
    data_base['mid'].nunique(), format_data_bowen['mid'].nunique(), format_data_bowen[format_data_bowen['pinglun']!=0].shape[0],
    len(first_class_pinglun.keys()), first_class_pinglun_df.shape[0], first_class_pinglun_df[first_class_pinglun_df['total_number']!=0].shape[0],
    len(second_class_pinglun.keys()), second_class_pinglun_df.shape[0]
))
# 博文id数量8591，获取到的博文详情数量8589，有1788条博文有评论, 获取到的一级评论的博文数量1788，获取到的一级评论量13878，有2075条一级评论有二级评论，获取到二级评论的一级评论数量2075，获取到的二级评论量4687
##########################################################################
# 提取评论所需字段
format_data_bowen.columns = ['博主昵称', '博主id', '博文id', '发文时间', '点赞数', '评论数', '转发数', '博文内容', '博文内容补充', '博文ip',
                             '图片数量', '视频观看量', '视频id', '关键词', 'url']
# 一级评论规范
first_class_pinglun_df_copy = first_class_pinglun_df[['created_at', 'id', 'user', 'analysis_extra', 'total_number',
                                                      'text_raw', 'like_counts', 'source']]
aa = first_class_pinglun_df.loc[0, 'user']
first_class_pinglun_df_copy['created_at'] = first_class_pinglun_df_copy['created_at'].apply(lambda funp: datetime.strptime(funp, GMT_FORMAT))
first_class_pinglun_df_copy['user1'] = first_class_pinglun_df_copy['user'].apply(lambda funp: funp['id'])
first_class_pinglun_df_copy['user2'] = first_class_pinglun_df_copy['user'].apply(lambda funp: funp['screen_name'])
first_class_pinglun_df_copy['gender'] = first_class_pinglun_df_copy['user'].apply(lambda funp: funp['gender'])


first_class_pinglun_df_copy['mid'] = first_class_pinglun_df_copy['analysis_extra'].apply(lambda funp:funp.split('|')[1].split(':')[1])
del first_class_pinglun_df_copy['user']
del first_class_pinglun_df_copy['analysis_extra']
first_class_pinglun_df_copy.columns = ['一级评论时间', '一级评论id', '二级评论数', '一级评论内容', '一级评论点赞量', '一级评论ip', '一级评论用户id',
                                       '一级评论用户昵称', '一级评论用户性别', '博文id']
first_class_pinglun_df_copy['博文id'].nunique()   # 比如这里1788对应实际的1753，部分博文显示有评论数量，但是实际不展示，造成的误差，比如mid '5074670235945397'，url   https://weibo.com/1677991972/000OvkvRoWFv




# 二级评论整理
second_class_pinglun_df_copy = second_class_pinglun_df[['created_at', 'id', 'user', 'rootid',
                                                      'text_raw', 'like_counts','source']]
second_class_pinglun_df_copy['user1'] = second_class_pinglun_df_copy['user'].apply(lambda funp: funp['id'])
second_class_pinglun_df_copy['user2'] = second_class_pinglun_df_copy['user'].apply(lambda funp: funp['screen_name'])
second_class_pinglun_df_copy['gender'] = second_class_pinglun_df_copy['user'].apply(lambda funp: funp['gender'])

del second_class_pinglun_df_copy['user']
second_class_pinglun_df_copy['created_at'] = second_class_pinglun_df_copy['created_at'].apply(lambda funp: datetime.strptime(funp, GMT_FORMAT))
second_class_pinglun_df_copy.columns = ['二级评论时间', '二级评论id',  '一级评论id', '二级评论内容','二级评论点赞量', '二级评论ip', '二级评论用户id',
                                       '二级评论用户昵称', '二级评论用户性别']





# 合并
format_data_bowen['博文id'] = format_data_bowen['博文id'].astype('str')
first_class_pinglun_df_copy['博文id'] = first_class_pinglun_df_copy['博文id'].astype('str')
hebing = pd.merge(format_data_bowen, first_class_pinglun_df_copy, on='博文id', how='outer')


hebing = pd.merge(hebing, second_class_pinglun_df_copy, how='outer', on='一级评论id')
hebing['发文时间'] = pd.to_datetime(hebing['发文时间'])
hebing.to_excel('{}汇总0.xlsx'.format(user_name), index=False)
save1 = hebing[~hebing['一级评论id'].isna()]


save1.to_excel('评论{}汇总.xlsx'.format(user_name), index=False)  # 这一个是博文详情、一级评论、二级评论的直接横向拼接结果
save1.columns




fisrt_select_columns = list(first_class_pinglun_df_copy.columns)
fisrt_select_columns.remove('二级评论数')
fisrt_select_columns.remove('博文id')
second_select_columns = list(second_class_pinglun_df_copy.columns)
second_select_columns.remove('一级评论id')
save2 = save1[list(format_data_bowen.columns) + fisrt_select_columns]
save2['pinglun_level'] = 1
save3 = save1[list(format_data_bowen.columns) + second_select_columns]
save3['pinglun_level'] = 2

common_columns = ['评论时间', '评论id',  '评论内容', '评论点赞量', '评论ip', '评论用户id',
                                       '评论用户昵称', '评论用户性别',  '评论级别']

save2.columns = list(format_data_bowen.columns) + common_columns
save3.columns = list(format_data_bowen.columns) + common_columns

save4 = pd.concat([save2, save3], axis=0, ignore_index=True)
save4 = save4.dropna(subset=['评论id'])
save4 = save4.drop_duplicates('评论id')
save4['评论级别'].value_counts()
second_class_pinglun_df_copy['二级评论id'].nunique()
save4['发文时间'] = pd.to_datetime(save4['发文时间'])


# save4.to_excel('{}汇总2.xlsx'.format(user_name), index=False)      # 这个是纵向拼接结果，


save1['二级评论id'].nunique()
save1['一级评论id'].nunique()
second_class_pinglun_df['id'].nunique()
second_class_pinglun_df['id'].value_counts()
save4.to_excel('{}_评论最终结果.xlsx'.format(user_name), index=False)  # 这个是纵向拼接结果



# aa = second_class_pinglun_df[second_class_pinglun_df['id']==5079213507874282]
# mid_to_url(5079213507874282)
# save4['date'] = save4['发文时间'].apply(lambda funp:str(funp)[:10])
# # author_uid:5044281310|mid:5078959434236706
# bb = format_data_bowen[format_data_bowen['博文id']=='5078959434236706']
# cc = first_class_pinglun_df[first_class_pinglun_df['analysis_extra'].str.contains('5078959434236706')]
# dd = second_class_pinglun_df[second_class_pinglun_df['analysis_extra'].str.contains('5078959434236706')]
# ee = first_class_pinglun_df[first_class_pinglun_df['id']==5078995375491543]



save4['博主id'].nunique()
save4['博文id'].nunique()

save1['博文id'].nunique()
hebing['博文id'].nunique()

save4['博文id'].nunique()


print('finish')

from base_setting import *


'''设置开始时间、结束时间，freq调整时间间隔，xx小时,核心控制内容
建议断点debug分部运行。先采集博文id，采集完毕后采集博文内容，采集完毕后再采集一级评论，完毕后再采集二级评论'''
start_dt = '2024-01-01'
end_dt = '2025-02-01'
date_series = pd.date_range(start=start_dt, end=end_dt, freq = '24H')
date_series = [str(date_i).split(':')[0]  for date_i in date_series]
date_series = ['-'.join(date_i.split(' '))  for date_i in date_series]
'''要把末尾天加上'''
if end_dt + '-00' in date_series:
    pass
else:
    date_series = date_series + [end_dt + '-00']

'''key_word放依次需要检索的关键词，topic为整个检索结果保存的文件名字'''
key_word = ['deepseek',]
topic = 'deepseek'


try:
    save_data = pd.read_csv('{}_bowen_info.csv'.format(topic), encoding='gb18030')
    finish_dt = list(save_data['finish_flag'].unique())
    save_data['mid'].nunique()
except:
    save_data = pd.DataFrame()
    finish_dt = []
bowen_info = []


'''tries, delay, timeout可调'''
@retry(tries=3, delay=3)
def myrequest2(params,  headers):
    response = requests.get('https://s.weibo.com/weibo', params=params, headers=headers,
                 timeout=10)
    print(response.status_code)
    return response


for key_word_i in key_word:
    for start_index, start_dt in tqdm(enumerate(date_series[:-1])):
        if str(key_word_i) + '_' + str(start_dt) in finish_dt:
            continue
        end_dt = date_series[start_index + 1]
        bowen_info_i = []
        '''不管有没有，每个词都检索50页，因为有些时候会很奇怪，比如第3页没有数据，但是第4页又有。
        这样检索效率会比较慢，但可以接受'''
        fengkong_flag = 0
        for page_i in tqdm(range(1, 51)):
            params = {
                'q': '{}'.format(key_word_i),
                'typeall': '1',
                'suball': '1',
                # 'timescope': 'custom:{}-0:{}-0'.format(start_dt, end_dt),
                'timescope': 'custom:{}:{}'.format(start_dt, end_dt),

                'Refer': 'g',
                'page': str(page_i)
            }
            # params = {
            #     'q': '{}'.format(key_word_i),
            #     'scope': 'ori',
            #     'suball': '1',
            #     'timescope': 'custom:{}:{}'.format(start_dt, end_dt),
            #     'Refer': 'g',
            #     'page': str(page_i)
            # }
            response = myrequest2(params,  headers)
            sleep(0.2)

            myhtml = etree.HTML(response.text)
            cookies_active = '微博搜索' in response.text
            if cookies_active:
                pass
            else:
                print('cookies过期')
                fengkong_flag = 1
                sleep(100)

                # raise ''
                break
            merror = myhtml.xpath('.//div[@class="m-error"]')
            if len(merror) > 0:
                bowen_list = []
            else:
                bowen_list = myhtml.xpath('.//div[@action-type="feed_list_item"]')
            print(
                '索引{}，{}_{}_{}当前日期搜索结果,第{}页, 博文数{}'.format(start_index, key_word_i, start_dt, end_dt,
                                                                          page_i,
                                                                          len(bowen_list)))
            # bowen_i = bowen_list[0]
            if bowen_list != 0:
                for bowen_i in bowen_list:
                    avtor = bowen_i.xpath('.//div[@class="avator"]/a/@href')[0]
                    mid = bowen_i.xpath('./@mid')[0]
                    # mid前面加一个字符串，免得存为csv时被当作数值造成结果错乱
                    bowen_info_i.append([avtor, 'mid' +str(mid), start_dt, key_word_i, str(key_word_i) + '_' + str(start_dt)])
                    aa = bowen_i.text
        if fengkong_flag:
            continue
        if len(bowen_info_i) == 0:
            bowen_info_i = [[np.nan, np.nan, start_dt, key_word_i, str(key_word_i) + '_' + str(start_dt)]]
        bowen_info = bowen_info + bowen_info_i

        save_data_i = pd.DataFrame(bowen_info, columns=['user_id', 'mid', 'dt', 'keyword', 'finish_flag'])
        save_data_i['user_id'] = save_data_i['user_id'].apply(lambda funp: mid_cut(funp))
        save_data = pd.concat([save_data, save_data_i], axis=0)

        save_data = save_data.drop_duplicates(subset=['mid', 'finish_flag']).reset_index(drop=True)
        '''采集50页存一次，并且程序中断了之后重新启动会跳过已采集的时间段，但是如果更改了start_dt/end_dt/freq,产生了新的时间区间，就会再采集'''
        save_data.to_csv('{}_bowen_info.csv'.format(topic), index=False, encoding='gb18030')







save_data = save_data.dropna().reset_index(drop=True)
save_data['mid'] = save_data['mid'].apply(lambda x: int(x[3:]))

save_data = save_data.drop_duplicates(subset=['mid'], keep='first').reset_index(drop=True)
save_data['id'] = save_data['mid'].apply(lambda funp: mid_to_url(funp))

save_data['keyword'].value_counts()

save_data['mid'].nunique()
save_data['dt'].value_counts()

print('采集到{}个博文'.format(save_data['mid'].nunique()))
#
from base_setting import *



save_data = pd.read_csv('{}_bowen_info.csv'.format(topic), encoding='gb18030')

save_data = save_data.dropna().reset_index(drop=True)
save_data['mid'] = save_data['mid'].apply(lambda x: int(x[3:]))

save_data = save_data.drop_duplicates(subset=['mid'], keep='first').reset_index(drop=True)
save_data['id'] = save_data['mid'].apply(lambda funp: mid_to_url(funp))

save_data['keyword'].value_counts()

save_data['mid'].nunique()
save_data['dt'].value_counts()


'''获取博文详情'''
try:
    f = open('./{}详情.json'.format(topic), 'r', encoding='utf-8')
    content = f.read()
    xiangqing_info = json.loads(content)
    finish_id = list(xiangqing_info.keys())
except:
    xiangqing_info = {}
    finish_id = []
oneid_index = 2

save_data = save_data[~save_data['id'].isin(finish_id)]


'''会报错，个别博文会因为前端显示原因一直采集失败，重复运行几次后仍然失败的博文，直接抛弃就行
博文内容和评论内容都保存了全量的json格式，后续可自定义解析'''
for oneid_index in tqdm(range(save_data.shape[0])):
    oneid = save_data.iloc[oneid_index, 5]
    if oneid in finish_id:
        continue
    params = {
        'id': oneid,
        'locale': 'zh-CN',
    }

    try:
        response1 = requests.get('https://weibo.com/ajax/statuses/show', params=params, headers=headers, timeout=20)
        sleep(0.2)
        rs1 = response1.json()
        response1.close()
        try:
            if rs1['message'] == '暂无查看权限':
                continue
        except Exception as e:
            pass
        if rs1['isLongText'] == True:
            params = {
                'id': oneid,
            }
            response2 = requests.get('https://weibo.com/ajax/statuses/longtext', params=params, headers=headers,
                                      timeout=20)
            sleep(0.1)
            rs2 = response2.json()
            response2.close()
        else:
            rs2 = {'data': {}, 'ok': 1}
        xiangqing_info[oneid] = [rs1, rs2]
    except Exception as e:
        print(e, rs1, oneid)
        if str(e) != "'isLongText'":
            sleep(5)


    # 将 json 数据写入文件
    if oneid_index % 100 == 0 and oneid_index != 0:
        data_xiangqing = json.dumps(xiangqing_info, ensure_ascii=False, indent=4)
        with open("{}详情.json".format(topic), "w", encoding='utf-8') as file:
            file.write(data_xiangqing)


data_xiangqing = json.dumps(xiangqing_info, ensure_ascii=False, indent=4)
with open("{}详情.json".format(topic), "w", encoding='utf-8') as file:
    file.write(data_xiangqing)
#
print('已经采集到的博文详情数量{}'.format(len(xiangqing_info.keys())))

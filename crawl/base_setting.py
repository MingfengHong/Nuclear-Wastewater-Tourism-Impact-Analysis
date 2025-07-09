import requests
import pandas as pd
from lxml import etree
from tqdm import tqdm
import numpy as np
from datetime import datetime
import json
from time import sleep
from retry import retry

'''
优化代码部分如果报错就重新运行，可能会反复出现，跟编码bug有关，暂时无解
'''

key_word = ['deepseek',]
topic = 'deepseek'
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'cache-control': 'max-age=0',
    # 登录后把cookies留着
    'cookie':
        'SINAGLOBAL=2688309647964.5464.1706081303953; UOR=,,cn.bing.com; SCF=Ak8DaDI_-le7QtkS6dd45Z8RpunSJ7lhnx-ADjz8C8K4ASJ0TNS-VJ-DyJJn_JJD8Egyj5hfJPDRvnliBo0fGns.; _s_tentry=weibo.com; Apache=330672512473.1868.1727940771214; ULV=1727940771224:34:1:2:330672512473.1868.1727940771214:1727664749518; ALF=1730636308; SUB=_2A25L-61EDeRhGeBI7VAZ9CfNzDWIHXVpeKCMrDV8PUJbkNANLVbBkW1NRmlCnDK0A34G1rHEO3ls6p2QcnbcXNen; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFjH9Hyy9DD60SNsUzmVjbV5JpX5KzhUgL.FoqcSozRSh.pS0.2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMcSoqE1hB4eKM4'
        ,
    'priority': 'u=0, i',
    'referer': 'https://www.weibo.com/',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0',
}

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
requests.DEFAULT_RETRIES = 5

def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def base62_decode(string, alphabet=ALPHABET):
    """Decode a Base X encoded string into the number

    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1

    return num

def mid_to_url(midint):
    # >>> mid_to_url(3501756485200075)
    # 'z0JH2lOMb'
    # >>> mid_to_url(3501703397689247)
    # 'z0Ijpwgk7'
    # >>> mid_to_url(3501701648871479)
    # 'z0IgABdSn'
    # >>> mid_to_url(3500330408906190)
    # 'z08AUBmUe'
    # >>> mid_to_url(3500247231472384)
    # 'z06qL6b28'

    midint = str(midint)[::-1]
    size = len(midint) / 7 if len(midint) % 7 == 0 else len(midint) / 7 + 1
    result = []

    for i in range(int(size)):
        s = midint[i * 7: (i + 1) * 7][::-1]
        s = base62_encode(int(s))
        s_len = len(s)
        if i < size - 1 and len(s) < 4:
            s = '0' * (4 - s_len) + s
        result.append(s)
    result.reverse()
    return ''.join(result)


def url_to_mid(url):
    # >> url_to_mid('z0JH2lOMb')
    # 3501756485200075L
    # >> url_to_mid('z0Ijpwgk7')
    # 3501703397689247L
    # >> url_to_mid('z0IgABdSn')
    # 3501701648871479L
    # >> url_to_mid('z08AUBmUe')
    # 3500330408906190L
    # >> url_to_mid('z06qL6b28')
    # 3500247231472384L

    url = str(url)[::-1]
    size = len(url) / 4 if len(url) % 4 == 0 else len(url) / 4 + 1
    result = []
    for i in range(int(size)):
        s = url[i * 4: (i + 1) * 4][::-1]
        s = str(base62_decode(str(s)))
        s_len = len(s)
        if i < size - 1 and s_len < 7:
            s = (7 - s_len) * '0' + s
        result.append(s)
    result.reverse()
    return int(''.join(result))





def mid_cut(x):
    try:
        x_new = ''.join(x.split('?')[0]).split('/')[-1]
    except:
        x_new = np.nan
    return x_new
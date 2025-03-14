import os
import requests
from urllib.parse import urlencode
import pandas as pd

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY = os.getenv("MOVIE_KEY")

def gen_url(dt="20210101", url_param={}):
    "호출 URL 생성, url_param 이 입력되면 multiMovieYn, repNationCd 처리"
    # params = {"key": KEY, "targetDt": dt}
    # params.update(url_param)
    # url = f"{BASE_URL}?{urlencode(params)}"
    #TODO = url_param 처리
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    for k, v in url_param.items():
        url = url + f"&{k}={v}"
    return url

def call_api(dt="20210101", url_param={}):
    url = gen_url(dt, url_param)
    data = requests.get(url)
    j = data.json()
    return j['boxOfficeResult']['dailyBoxOfficeList']

def list2df(data: list, dt: str):
    df = pd.DataFrame(data)
    df['dt'] = dt
    return df

def save_df(df: pd.DataFrame, base_path) -> str:
    df.to_parquet(base_path, partition_cols=['dt'])
    save_path = f"{base_path}/dt={df['dt'][0]}"
    return save_path
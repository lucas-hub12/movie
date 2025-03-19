import os
import requests
from urllib.parse import urlencode
import pandas as pd

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY = os.getenv("MOVIE_KEY")

def gen_url(dt="20210101", url_param={}):
    "호출 URL 생성, url_param 이 입력되면 multiMovieYn, repNationCd 처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    for k, v in url_param.items():
        url = url + f"&{k}={v}"
    return url

def call_api(dt="20210101", url_param={}):
    url = gen_url(dt, url_param)
    data = requests.get(url)
    j = data.json()
    return j['boxOfficeResult']['dailyBoxOfficeList']

def list2df(data: list, dt: str, url_params={}):
    df = pd.DataFrame(data)
    df['dt'] = dt
    for k,v in url_params.items():
        df[k] = v 
    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
                'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten',
                'salesChange',   'audiInten', 'audiChange']
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    return df

def save_df(df, base_path, partitions=['dt']):
    df.to_parquet(base_path, partition_cols=partitions)
    save_path = base_path
    for p in partitions:
        save_path = save_path + f"/{p}={df[p][0]}"
    return save_path

def fill_na_with_column(origin_df, c_name):
    df = origin_df.copy()
    for i, row in df.iterrows():
        if pd.isna(row[c_name]):
            same_movie_df = df[df["movieCd"] == row["movieCd"]]
            notna_idx = same_movie_df[c_name].dropna().first_valid_index()
            if notna_idx is not None:
                df.at[i, c_name] = df.at[notna_idx, c_name]
    return df

def gen_unique(df: pd.DataFrame, drop_columns: list) -> pd.DataFrame:
    df_drop = df.drop(columns=['rnum', 'rank', 'rankInten', 'salesShare'])
    unique_df = df_drop.drop_duplicates()
    return unique_df

def re_ranking(df: pd.DataFrame) -> pd.DataFrame:
    df["rnum"] = df["audiCnt"].rank(method="dense", ascending=False).astype(int)
    df["rank"] = df["audiCnt"].rank(method="min", ascending=False).astype(int)
    return df

def fill_unique_ranking(df: pd.DataFrame, dt:str) -> pd.DataFrame:
    df1 = fill_na_with_column(df, 'multiMovieYn')
    df2 = fill_na_with_column(df1, 'repNationCd')
    drop_columns=['rnum', 'rank', 'rankInten', 'salesShare']
    unique_df = gen_unique(df=df2, drop_columns=drop_columns)
    new_ranking_df = re_ranking(unique_df)
    new_ranking_df['dt'] = dt
    return new_ranking_df
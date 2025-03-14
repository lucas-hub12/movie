from movie.api.call import gen_url, call_api, list2df
import os
import pandas as pd

def test_gen_url_default():
   a = gen_url()
   print(a)
   assert "kobis" in a
   assert "targetDt" in a
   assert os.getenv("MOVIE_KEY") in a

def test_gen_url():
    b = gen_url(url_param={"multiMovieYn": "Y", "repNationCd":"K"})
    print(b)
    assert "&multiMovieYn=Y" in b
    assert "&repNationCd=K" in b

def test_call_api():
    c = call_api()
    print(c)
    assert isinstance(c,list)
    assert isinstance(c[0]['rnum'],str)
    assert len(c) == 10
    for e in c:
        assert isinstance(e, dict)
    
def test_list2df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    assert isinstance(df, pd.DataFrame)
    assert len(data) == len(df)
    assert set(data[0].keys()).issubset(set(df.columns))
    assert "dt" in df.columns, "df 컬럼이 있어야 함"
    assert (df["dt"] == ymd).all(), "모든 컬럼에 입력된 날짜 값이 존재 해야 함"
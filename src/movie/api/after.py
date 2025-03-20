import pandas as pd
import os

def fillna_meta(previous_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    # print("=== fillna_meta 시작 ===")
    # print("이전 DataFrame (previous_df):")
    # print(previous_df)
    # print("현재 DataFrame (current_df):")
    # print(current_df)
    if previous_df is None:
        # result = current_df[["movieCd", "multiMovieYn", "repNationCd"]]
        # print("이전 DataFrame이 None이므로 current_df의 특정 컬럼만 반환:")
        # print(result)
        return current_df[["movieCd", "multiMovieYn", "repNationCd"]]
        
        # movieCd 기준으로 outer join
        merged_df = previous_df.merge(current_df, on="movieCd", how="outer", suffixes=("_A", "_B"))
        print(merged_df)

        # NaN 값 대체 (A 값 우선, 없으면 B 값 사용)
        merged_df["multiMovieYn"] = merged_df["multiMovieYn_A"].combine_first(merged_df["multiMovieYn_B"])
        merged_df["repNationCd"] = merged_df["repNationCd_A"].combine_first(merged_df["repNationCd_B"])

        # 불필요한 컬럼 제거
        meta_df = merged_df[["movieCd", "multiMovieYn", "repNationCd"]]
        print("최종 meta DataFrame (meta_df):")
        print(meta_df)
    
        print("=== fillna_meta 종료 ===")
        return meta_df
    
def read_df_or_none(parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
    else:
        df = None
        
    return df



def save_with_mkdir(df: pd.DataFrame, parquet_path: str) -> str:
    """디렉토리가 없으면 생성하고, DataFrame을 parquet 파일로 저장한 후 경로 반환"""
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    
    # DataFrame을 parquet 형식으로 저장
    df.to_parquet(parquet_path)
    
    # 저장된 파일의 절대 경로 반환
    absolute_path = os.path.abspath(parquet_path)
    print(f"파일이 성공적으로 저장되었습니다: {absolute_path}")
    
    return absolute_path
import pandas as pd

def generate_data_path(execution_date):
        date_str = execution_date.strftime("%Y/%m/%d/%H")
        return f"/home/sgcho/data/{date_str}"


def f_agg_data():
    data = pd.read_parquet(f"{generate_data_path()}/data.csv", engine='pyarrow') # data.parquet 읽기
    df = pd.DataFrame(data)  # DataFrame 생성
    group_df = df.groupby(["value"]).count().reset_index() # DataFrame을 Parquet 파일로 저장
    group_df.to_csv(f"{generate_data_path()}/agg.csv)")


# def f_agg_data():
        
#     base_path ="/home/jacob/data/"
#     parquet_file = f"{base_path}{dis_path}/data.parquet"
#     agg_file = f"{base_path}{dis_path}/agg.csv"
    
#     if not os.path.exists(parquet_file):
#         raise FileNotFoundError(f"Parquet파일이 아래 주소에 존재하지 않습니다: {parquet_file}")
    
#     df = pd.read_parquet(parquet_file, engine='pyarrow')
#     group_df = df.groupby(["value"]).count().reset_index()
#     try:
#         group_df.to_csv(agg_file, index=False)
#         return f" CSV 파일이 아래 경로에 생성되었습니다: {agg_file}"
#     except Exception:
#         return "파일 생성 중 오류가 발생했습니다"
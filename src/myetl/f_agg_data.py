from datetime import datetime
import pandas as pd
import pendulum

execution_date = pendulum.datetime(2025, 3, 12, tz="Asia/Seoul")

def generate_data_path(path):
    return f"/home/sgcho/data/{path}"

def f_agg_data(path):
    data_path = generate_data_path(path)
    data = pd.read_parquet(f"{data_path}/data.csv", engine='pyarrow') # data.parquet 읽기
    df = pd.DataFrame(data)  # DataFrame 생성
    group_df = df.groupby(["value"]).count().reset_index() # groupby value
    agg_path = f"{data_path}/agg.csv"
    group_df.to_csv(f"{data_path}/agg.csv")  # agg.csv 로 저장
    return f"{agg_path}에 파일이 생성되었습니다"


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
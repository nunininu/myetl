from datetime import datetime
import pandas as pd
import pendulum

execution_date = pendulum.datetime(2025, 3, 12, tz="Asia/Seoul")

def generate_data_path(path):
    return f"/home/sgcho/data/{path}"

def f_load_data(path):
    data_path = generate_data_path(path)
    data = pd.read_csv(f"{data_path}/data.csv") # data.csv 읽기
    df = pd.DataFrame(data)  # DataFrame 생성
    parquet_path = f"{data_path}/data.parquet"
    df.to_parquet(parquet_path, engine="pyarrow") # DataFrame을 Parquet 파일로 저장
    return f"{parquet_path}에 파일이 생성되었습니다"

# Parquet 파일을 읽어 DataFrame으로 로드
# df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')






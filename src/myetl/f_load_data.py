from datetime import datetime
import pandas as pd
import pendulum

execution_date = pendulum.datetime(2025, 3, 12, tz="Asia/Seoul")

def generate_data_path(execution_date):
    date_str = execution_date.strftime("%Y/%m/%d/%H")
    return f"/home/sgcho/data/{date_str}"

def f_load_data():
    data_path = generate_data_path(execution_date)
    data = pd.read_csv(f"{data_path}/data.csv") # data.csv 읽기
    df = pd.DataFrame(data)  # DataFrame 생성
    df.to_parquet('data.parquet', engine="pyarrow") # DataFrame을 Parquet 파일로 저장

# Parquet 파일을 읽어 DataFrame으로 로드
df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')



# 출력
print(df_loaded)


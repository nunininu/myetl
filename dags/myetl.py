from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
import pendulum

# Directed Acyclic Graph
with DAG(
    "myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # data.csv 생성 경로 하드코딩 수정 (gpt)
    def generate_data_path(execution_date):
        date_str = execution_date.strftime("%Y/%m/%d/%H")
        return f"/home/sgcho/data/{date_str}"
    
    # data.csv 생성 명령어 수정 (gpt)
    make_data = BashOperator(
        task_id="make_data", 
        bash_command="""
        bash /home/sgcho/airflow/make_data.sh {{ params.data_path }}
        """,
        params={"data_path": "{{ execution_date.strftime('%Y/%m/%d/%H') }}"}
    )
    
    # 수정 전 구 코드
    # make_data = BashOperator(
    #     task_id="make_data", 
    #     bash_command="""
    #     /home/sgcho/airflow/make_data.sh /home/sgcho/data/2025/03/12/01
    #     """
    #     )
    
    # def f_agg_data(): def f_load_data(): 는 별도의 파일로 분리하였음
            
    load_data = PythonVirtualenvOperator(
            task_id="load_data", python_callable=f_load_data, 
            requirements=["git+https://github.com/nunininu/myetl.git@0.1.2"]    
        )
    
    agg_data = PythonVirtualenvOperator(
            task_id="agg_data", python_callable=f_agg_data, 
            requirements=["git+https://github.com/nunininu/myetl.git@0.1.2"]            
        )
    
start >> make_data >> load_data >> agg_data >> end



###=================================== 별도 파일로 분리 (시작)=============================###
    # def f_load_data():
    #     import pandas as pd
    #     data = pd.read_csv("/home/sgcho/data/2025/03/12/01/data.csv")
    #     df = pd.DataFrame(data)   
        
    #     # DataFrame을 Parquet 파일로 저장
    #     df.to_parquet('data.parquet', engine='pyarrow')

    #     # Parquet 파일을 읽어 DataFrame으로 로드
    #     df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')

    #     # 출력
    #     print(df_loaded)
    ###=================================== 별도 파일로 분리 (끝)=============================###
    
    ###=================================== 별도 파일로 분리 (시작)=============================###
    # def f_agg_data():
    #     query = """
    #     SELECT
    #         l.menu_name,
    #         m.name,
    #         l.dt
    #     FROM member m
    #     INNER JOIN lunch_menu l
    #     on l.member_id = m.id
            
    #     """
    #     conn = get_connection()
    #     cursor = conn.cursor()
    #     cursor.execute(query)
    #     rows = cursor.fetchall()
    #     cursor.close()
    #     conn.close()
    ###=================================== 별도 파일로 분리 (끝)=============================###




# make_data = BashOperator(
#         task_id="make_data", 
#         bash_command="""
#         /home/sgcho/airflow/make_data.sh /home/sgcho/data/2025/03/12/01
#         # execution_date_kst="{{ execution_date.in_tz('Asia/Seoul') }}"
#         # year=$(date -d "$execution_date_kst" +%Y)
#         # month=$(date -d "$execution_date_kst" +%m)
#         # day=$(date -d "$execution_date_kst" +%d)
#         # hour=$(date -d "$execution_date_kst" +%H)
#         # mkdir -p ~/data/sgcho/$year/$month/$day/$hour
#         """
#         )


# 샘플 DataFrame 생성
# data = {
#     'id': [1, 2, 3, 4, 5],
#     'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
#     'value': [100, 200, 300, 400, 500]
# }

# df = pd.DataFrame(data)

# # DataFrame을 Parquet 파일로 저장
# df.to_parquet('data.parquet', engine='pyarrow')

# # Parquet 파일을 읽어 DataFrame으로 로드
# df_loaded = pd.read_parquet('data.parquet', engine='pyarrow')

# # 출력
# print(df_loaded)







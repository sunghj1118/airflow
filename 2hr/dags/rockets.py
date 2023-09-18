import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# 실행 순서를 정의하기 위한 전체적인 DAG instance를 생성하는 코드
myDag = DAG(
    dag_id="download_rocket_launches",
    description="Download rocket pictures of recently launched rockets.",
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval="@daily",
)

# DAG instance의 Operator(Task) 노드 ① - https://ll.thespacedevs.com/2.0.0/launch/upcoming 사이트에서 로켓 발사 데이터를 다운로드하는 bash(터미널) 명령어을 수행하는 Bash Operator(Task)
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=myDag,
)



# PythonOperator에서 수행하기 위한 동작을 파이썬 함수 형태로 정의하는 코드
def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")



# DAG instance의 Operator(Task) 노드 ② - 지정한 함수 형태의 파이썬 코드를 실행하는 작업을 수행하는 Python Operator(Task)
get_pictures = PythonOperator(
    task_id="get_pictures", 
    python_callable=_get_pictures, 
    dag=myDag
)

# DAG instance의 Operator(Task) 노드 ③ - /tmp/images/ 디렉토리 내에 있는 저장된 이미지 수를 세고, 그 결과를 출력하는 bash(터미널) 명령어를 수행하는 Bash Operator(Task)
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=myDag,
)

# DAG instance를 구성하는 Operator(Task)의 실행 순서를 정하는 코드
download_launches >> get_pictures >> notify
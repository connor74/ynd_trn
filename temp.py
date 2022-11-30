import os
import requests
import time

directory_stage = "stage"

#parent_dir = "/lessons/"
#path = os.path.join(parent_dir, directory_stage)
#os.mkdir(path)
API_KEY = "5f55e6c0-e9e5-4a9c-b313-63c01fc31460"
NICKNAME = "kurzanovart"
COHORT = "8"
URL_API = "https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/"



generate_report_response = requests.post(
    f"{URL_API}/generate_report",
    headers={
        "X-API-KEY": API_KEY,
        "X-Nickname": NICKNAME,
        "X-Cohort": COHORT
    }
).json()
task_id = generate_report_response["task_id"]
while True:
    get_report_response = requests.get(
        f"{URL_API}/get_report?task_id={task_id}",
        headers={
        "X-API-KEY": API_KEY,
        "X-Nickname": NICKNAME,
        "X-Cohort": COHORT
        }
    ).json()
    if get_report_response['status'] == 'SUCCESS':
        report_id = get_report_response['data']['report_id']
        break
    else:
        time.sleep(10)
print(report_id)
print(get_report_response)
files = [
    "customer_research.csv",
    "user_order_log.csv",
    "user_activity_log.csv",
]

for file in files:
    url = f"https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/{report_id}/{file}"
    

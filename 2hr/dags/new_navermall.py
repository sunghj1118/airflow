from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

import requests
import json
import pandas as pd

# Setup information
headers = {
    'authority': 'search.shopping.naver.com',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    # 'cookie': 'NNB=UABBCFMBWELGI; SHP_BUCKET_ID=1; _ga=GA1.2.869870948.1686143985; NV_WETR_LAST_ACCESS_RGN_M="MDk2NTAxMDE="; NV_WETR_LOCATION_RGN_M="MDk2NTAxMDE="; autocomplete=use; nx_ssl=2; ASID=70d4fe4b0000018962c0299000000059; nid_inf=-1340535807; NID_AUT=jf/4Y3DfCGMNU3p6Qf5Fmwzzf/w4E+iKya2muh9idQpixay8XEzkjflthSs6cerU; NID_JKL=Y3lq5EyM3RcXdunr0NQAPesV9Ijl06s+yhMLWNoyaDc=; ncpa=95694|lkc5kark|7afcd5e52f726184f10a4ec9147670f9d737ba55|95694|38d9db0b614154e981ea08414cd22bddb7580d74:628445|lkgkqhsw|8f9442c14a7d6aa961de6424960bed2291aa4341|s_1e55892d53fc6|e37402fb9f173df017d3d18b6a065dea9267d898; spage_uid=i7wthsprvxssseA2aV0ssssstu0-480531; NID_SES=AAABrLn6sWP6UThaLUU3CvFMGShBlmYo/8RiMRih440d9pmpH5gGHofowuziy+CXG12LAncUsid966/5unRMEKZ4WEb4gH0kZAFDw/1vuh2h9vBCwj31ICK2erNrNB5ilng3dDKHPXXcJsFtfPP1DdRLp+UtYYgXgy4CVOjTzOR901RhNUky8wFM6yqqgzf90zRP60/bitb2gT8p3lZ5db+sSaOj9Oki2uy6OuwfLrHUOGcUOFw297sshv5kEnBYefoirLzRbHFJIVPSR5LlwzogEs63AxQCWn0jdq/729Tpvv37ZLnLpJK8AohXZQ2T1s5v2zUbldKQw1CvJbHQthf4Dqslv7ToWKqdj6qMkbFXInmRIBEYshp3CEKb0BNclj/HwDPthTBMxZxBesIQg2DSpNOa6yoMqr6yQU1vGmSWyYLOOWHcMzIbJrmQ8CpCKdtkN4ITSQI/wINFdFTU/NupRUxGp1x1EZxDVW2D+IH02JoIMm70NHnnbUt9gFPd9MP++OmzH42/78w3eY+7f3ZODQ6AQAD4oHfac57K/sxtFuX35QZ73bCWAeGB77I0f7AXsw==; page_uid=i79q1dprvhGsshwrtnGssssssnG-329409',
    'logic': 'PART',
    'referer': 'https://search.shopping.naver.com/search/category/100004258?catId=50000028&frm=NVSHCHK&origQuery&pagingIndex=2&pagingSize=40&productSet=checkout&query&sort=review&timestamp=&viewType=list',
    'sbth': '33c4f688d96c789665c034d083837dd58fa2c46c1b51db47832ed0efffbd4b3b3f18313efb7fe4c60ed1f6942b391187',
    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'sec-ch-ua-arch': '"arm"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version-list': '"Not.A/Brand";v="8.0.0.0", "Chromium";v="114.0.5735.198", "Google Chrome";v="114.0.5735.198"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"macOS"',
    'sec-ch-ua-platform-version': '"12.0.1"',
    'sec-ch-ua-wow64': '?0',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
}

# DAG information
default_args = {
    'owner': 'hyunjoon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# URL
## http://35.208.123.6:8080
api_url = "http://35.208.123.6:8080/"
api_headers = {
    "Content-Type": "application/json;"
}


# [Product] GET API / Product table 에서 데이터 조회
# url: "http://127.0.0.1:8080/products"
def GetProducts():
    response = requests.get(api_url+"products", headers=api_headers).json()
    print(response)
    return response

# [Product] POST API / Product table 에 데이터 삽입
# url: "http://35.208.123.6:8080/products"
def PostProducts(products):
    response = requests.post(api_url+"products", headers=api_headers, data=products.encode('utf-8'))

# [Product] POST API / Product table 에 데이터 삽입
def load_json() :
    params = {
        'catId': '50000028',
        'eq': '',
        'frm': 'NVSHTTL',
        'iq': '',
        'pagingIndex': '1', 
        'pagingSize': '80',
        'productSet': 'total',
        'sort': 'review',
        'viewType': 'list',
        'window': '',
        'xq': '',
    }

    response = requests.get(
        'https://search.shopping.naver.com/api/search/category/100004258',
        params=params,
        headers=headers,
    )

    itemlist = json.loads(response.text)

    return itemlist['shoppingResult']['products']

# loads JSON data and returns it as a DataFrame
def get_product() :
    data = load_json()
    df = pd.DataFrame(data)

    data_list = pd.DataFrame(
        {   #product table
            'store_id' : df['mallId'],
            'product_id' : df['id'], 
            'productURl' : df['mallProductUrl'],
            'product_name' : df['productTitle'],
            'price' : df['price'], 
            'delivery_price' : df['deliveryFeeContent'],
            'product_amount' : df['purchaseCnt'],
            'review' : df['reviewCount'],
            'review_score' : df['scoreInfo'], #평점
            'heart' : df['keepCnt'],
            'registerdate' : df['openDate'],
        }
    )

    js = data_list.to_json(orient = 'records', force_ascii=False, indent=4)
    print(js)
    PostProducts(js)

# Returs the data in Store table as a DataFrame
def get_store() :
    data = load_json()
    df = pd.DataFrame(data)

    data_list = pd.DataFrame(
        {   #store table
            'product_id' : df['id'],
            'store_id' : df['mallId'],
            'storeURL' : df['mallPcUrl'],
            'store_name' : df['mallName'],
            #'store_cache = df['mallInfoCache'],
            'category1' : df['category1Name'], #대분류
            'category2' : df['category2Name'], #중분류
            'category3' : df['category3Name'], #소분류
            # good_service = i['goodService'],
        }
    )

    js = data_list.to_json(orient = 'records', force_ascii=False, indent=4)
    print(js)
    return js

def naver_crawl():
    print("NAVER_CRAWL: START")

with DAG(
    default_args=default_args,
    dag_id="naver_mall_DAG_NEW4",
    start_date=datetime(datetime.now),
    schedule_interval='*/5 * * * *'
) as dag:
    task1 = PythonOperator(
        task_id='data_crawling_task',
        python_callable=naver_crawl
    )

    task2 = PythonOperator(
        task_id='get_data',
        python_callable=get_product
    )

    task3 = PythonOperator(
        task_id='get_products',
        python_callable=GetProducts
    )

    task4 = PythonOperator(
        task_id='get_store',
        python_callable=get_store
    )

    task1 >> task2 >> task3 >> task4


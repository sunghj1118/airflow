from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

import requests
import json
import pandas as pd


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


default_args = {
    'owner': 'hyunjoon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def load_json(number) :
    pageingIndex = number

    params = {
        'catId': '50000028',
        'eq': '',
        'frm': 'NVSHTTL',
        'iq': '',
        'pagingIndex': pageingIndex, 
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

    try:
        for data in response.iter_content(chunk_size=1024):
            print(data)
    except ChunkEncodingError as ex:
        print(f"Invalid chunk encoding {str(ex)}")

    return response

def isRepeat(previousItemList, itemList) :
    
    #같은 값을 응답받으면 True 리턴
    if previousItemList['shoppingResult']['products'][0]['productName'] == itemList['shoppingResult']['products'][0]['productName']:
        print('------------------------finish------------------------')
        return True

    return False

def printData(itemList) :
    # 추출하려는 값의 key를 입력
    for i in itemList['shoppingResult']['products']:
      title = i['productName']
      price = i['price'] 
      print('productName=',title,'price=',price)


def get_data() :
# 중복 체크를 위한 변수
    previousItemList = []
    product_table = pd.DataFrame(columns=[
            'store_id',
            'product_id',
            'productURl',
            'product_name',
            'price', 
            'delivery_price',
            'product_amount',
            'review',
            'review_score', #평점
            'heart',
            'registerdate', 
            ])

    store_table = pd.DataFrame(columns = [
            'store_id',
            'storeURL',
            'store_name',
            # store_grade,
            'category1', #대분류
            'category2', #중분류
            'category3', #소분류
            #store_amount,
            #store_sales,
            # good_service,
            ])


    number = 1
    while number < 10000000 :
        print('number = ',number)
        response = load_json(number)
        itemList = json.loads(response.text)
        
        # 첫번째 호출에는 list가 비교가 안되니 continue를 한다.
        if number == 1 :
            number = number + 1
            previousItemList = itemList
            printData(itemList)
            continue
        
        # 반복 응답 Check Method
        # 응답이 같은 값으로 반복되었는지 확인하는 메서드를 실행한다. True일 경우 중복이라서 break
        if isRepeat(previousItemList, itemList) :
            break
        
        previousItemList = itemList
        
        printData(itemList)

        data = itemList['shoppingResult']['products']
        df = pd.DataFrame(data)


        product_temp = pd.DataFrame(
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

        store_temp = pd.DataFrame(
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
        product_table = pd.concat([product_table,product_temp])
        store_table = pd.concat([store_table,store_temp])

        number = number + 1

    product_table.reset_index(inplace=True)
    store_table.reset_index(inplace=True)

    product_js = product_table.to_json(orient = 'records', force_ascii=False, indent=4)
    store_js = store_table.to_json(orient = 'records', force_ascii=False, indent=4)

    # Download data to csv
    # product_table.to_csv('/Users/hyunjoon/Projects/airflow/2hr/store_data.csv')
    # store_table.to_csv('/Users/hyunjoon/Projects/airflow/2hr/store_data.csv')

    return product_js, store_js







def naver_crawl():
    print("This is a test")

with DAG(
    default_args=default_args,
    dag_id="naver_mall_DAG4",
    start_date=datetime(2023, 9, 13),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PythonOperator(
        task_id='data_crawling_task',
        python_callable=naver_crawl
    )

    task2 = PythonOperator(
        task_id='get_data',
        python_callable=get_data
    )

    task1 >> task2


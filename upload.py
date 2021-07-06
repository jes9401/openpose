# -*- coding: utf-8 -*-
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import glob
import time
import pandas as pd
import re
import datetime

# kinect 좌표 데이터(csv)가 저장되는 경로
image_path = './image/'

while True:
    # csv 파일들만 리스트에 저장
    image_list = glob.glob(os.path.join(image_path,'*.csv'))
    image_list2 = image_list.copy()

    image_list = list(map(lambda x:x.split('\\')[-1],image_list))


    try:
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

        user = 'kinect-test'
        
        # 연결하는 부분
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(user)
        print(user,'컨테이너 연결 완료')
    except Exception as ex:
        print('Exception:')
        print(ex)

    blob_list = container_client.list_blobs()

    for image in image_list:
        blob_list = container_client.list_blobs()
        # 각 파일의 이름과 수정 시간으로 변환
        # +9를 한 이유는 UTC이기 때문
        blob_list = list(map(lambda x:[x.name,"".join(re.findall('\d',str(x.last_modified+datetime.timedelta(hours=9))[:16]))],blob_list))
        blob_name_list = []
        blob_time_list = []

        # 파일 이름과 수정 시간 리스트 나누기 (수정 시간은 원래 사용하려 했는데 사용하지 않기로 함)
        for x in blob_list:
            blob_name_list.append(x[0])
            blob_time_list.append(x[1])

        upload_file_path = os.path.join(image_path,image)
        blob_client = blob_service_client.get_blob_client(container=user, blob=image)
        
        # csv 파일 이름이 blob_name_list에 없으면 업로드
        if image not in blob_name_list:
            with open(upload_file_path,"rb") as data:
                blob_client.upload_blob(data,overwrite=True)
            print(image,'upload')
        
        # csv 파일 이름이 blob_name_list에는 있지만 
        # (현재 시간 - 마지막으로 수정된 시간)이 10보다 작으면 오버라이트 함
        else:
            x = image_list2[blob_name_list.index(image)]
            # y = datetime.datetime.fromtimestamp(os.path.getmtime(x))+datetime.timedelta(seconds=120)
            y = datetime.datetime.fromtimestamp(os.path.getmtime(x))
            now = datetime.datetime.now()
            temp = now-y
            
            if temp.seconds<10:
                with open(upload_file_path,"rb") as data:
                    blob_client.upload_blob(data,overwrite=True)
                print(image,'upload')        
    # 임의로 5초씩 딜레이 
    # 가끔 퍼미션 디나이 뜨는데 더 찾아봐야될듯 함
    time.sleep(5)
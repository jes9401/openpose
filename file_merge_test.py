import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import glob
import time
import pandas as pd

# cmd에 이 명령어 실행하고 진행하세요. 연결 문자열 저장하는 내용입니다
# setx AZURE_STORAGE_CONNECTION_STRING "DefaultEndpointsProtocol=https;AccountName=hysdb;AccountKey=P3u69Z/annyhB5+73QfQw8P7Ks+r9anFxaYzBkTz7mgFWssNAHMYJdov7692Q/3PS5D39MUOZY+DYdz0OmkJ9A==;EndpointSuffix=core.windows.net"
# setx AZURE_STORAGE_CONNECTION_STRING "DefaultEndpointsProtocol=https;AccountName=hysdb2;AccountKey=5sUVtfucTcwdJ/cf3GgiCiOS4BecVe2pyjx3pJ8S34XW2JZIp5ebCl8JQx4OeggT5/rmmb7L0btZOPWhhIUtOg==;EndpointSuffix=core.windows.net"


# cmd에 이 명령어 실행하세요
# pip install azure-storage-blob


local_path='./test/'
all_path = './all/'
gyro_list = glob.glob(os.path.join(local_path, '*GYROSCOPE.csv'))
gyro_list = list(map(lambda x:x.split('\\')[-1],gyro_list))
        
acc_list = glob.glob(os.path.join(local_path, '*ACCELEROMETER.csv'))
acc_list = list(map(lambda x:x.split('\\')[-1],acc_list))

all_file_list = glob.glob(os.path.join(all_path,'*ALL.csv'))
acc_list = list(map(lambda x:x.split('\\')[-1],all_file_list))
while True:
    try:
        # 연결 문자열
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

        #  'jes' 를 본인 Blob containers 이름으로 변경하면됨
        user = 'jes'
        
        # 연결하는 부분입니다. 
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(user)

    except Exception as ex:
        print('Exception:')
        print(ex)
        continue
    
    # Blob containers의 파일 리스트
    blob_list = container_client.list_blobs()
        
    gyro = 'GYROSCOPE.txt'
    acc = 'ACCELEROMETER.txt'
    
    for blob in blob_list:
        if gyro in blob.name and blob.name[:-3]+"csv" not in gyro_list:
            # blob_client => user 컨테이너의 blob.name(파일 이름)
            blob_client = blob_service_client.get_blob_client(container=user, blob=blob.name)

            # 파일 다운로드 경로 => 위에서 선언한 local_path + 파일이름(blob.name)
            download_file_path = os.path.join(local_path, blob.name[:-3]+"csv")
            # 그냥 무슨 파일 다운로드하는지 확인하는 출력문
            print("\nDownloading blob to \n\t" + download_file_path)

            # 파일 생성
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            gyro_list.append(blob.name[:-3]+"csv")
                
        elif acc in blob.name and blob.name[:-3]+"csv" not in acc_list:
            blob_client = blob_service_client.get_blob_client(container=user, blob=blob.name)
            download_file_path = os.path.join(local_path, blob.name[:-3]+"csv")
            print("\nDownloading blob to \n\t" + download_file_path)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            acc_list.append(blob.name[:-3]+"csv")

    
    temp_list = acc_list+gyro_list
    for acc_file in acc_list:
        temp = acc_file[:-17]
        acc_temp = acc_file
        gyro_temp = temp+"GYROSCOPE.csv"
        if temp+"GYROSCOPE.csv" in gyro_list and temp+"ALL.csv" not in all_file_list:
            df_acc = pd.read_csv(local_path+acc_temp)
            df_gyro = pd.read_csv(local_path+gyro_temp)
            time_list = list(df_acc['Time'])
            df_acc['Time'] =list(map(lambda x:"".join(x.split(':')[:-1])+x.split(':')[-1][:2],time_list))
            df_acc.drop('TimeZone',axis=1,inplace=True)
            df_acc.drop_duplicates(['Time'],inplace=True)

            time_list = list(df_gyro['Time'])
            df_gyro['Time'] =list(map(lambda x:"".join(x.split(':')[:-1])+x.split(':')[-1][:2],time_list))
            df_gyro.drop('TimeZone',axis=1,inplace=True)
            df_gyro.drop_duplicates(['Time'],inplace=True)
            df_all = pd.merge(df_acc,df_gyro,on='Time')
            df_all.to_csv(all_path+temp+"ALL.csv",index=False)
            print("create",temp+"ALL.csv")
            all_file_list.append(temp+"ALL.csv")
        else:
            pass
            
        
    time.sleep(3)

# 전체 삭제
#elif n == 2:
#    print("진짜 삭제하려면 엔터를 눌러주세요")
#    input()
#    try:
#        print("삭제중...")
#        for blob in blob_list:
#            if blob.name == 'data/registry.da':
#                print('data는 삭제 안해용~')
#           else:
#                print(blob.name,'삭제')
#                container_client.delete_blobs(blob.name)
#        print('삭제완료')
#           
#    except Exception as ex:
#        print('삭제 실패')
#        print('Exception:')
#        print(ex)

    

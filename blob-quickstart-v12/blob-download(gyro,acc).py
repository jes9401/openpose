import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import glob




# cmd에 이 명령어 실행하고 진행하세요. 연결 문자열 저장하는 내용입니다
# setx AZURE_STORAGE_CONNECTION_STRING "DefaultEndpointsProtocol=https;AccountName=hysdb;AccountKey=P3u69Z/annyhB5+73QfQw8P7Ks+r9anFxaYzBkTz7mgFWssNAHMYJdov7692Q/3PS5D39MUOZY+DYdz0OmkJ9A==;EndpointSuffix=core.windows.net"

# cmd에 이 명령어 실행하세요
# pip install azure-storage-blob

try:
    # 걍 버전 출력
    print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")

    # 연결 문자열 확인
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    print("연결 문자열 = ",connect_str)


    #  'jes' 를 본인 Blob containers 이름으로 변경하면됨
    user = 'jes'
    
    # 연결하는 부분입니다. 
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(user)

    # 파일을 실제로 저장할 경로 지정하면됩니다.
    local_path='./file/'

    # Blob containers의 파일 리스트
    blob_list = container_client.list_blobs()        

    
    gyro = 'GYROSCOPE.txt'
    acc = 'ACCELEROMETER.txt'

    gyro_list = glob.glob(os.path.join(local_path, '*GYROSCOPE.txt'))
    gyro_list = list(map(lambda x:x.split('\\')[-1],gyro_list))
    
    acc_list = glob.glob(os.path.join(local_path, '*ACCELEROMETER.txt'))
    acc_list = list(map(lambda x:x.split('\\')[-1],acc_list))
    
    for blob in blob_list:
        if gyro in blob.name and blob.name not in gyro_list:
            # blob_client => user 컨테이너의 blob.name(파일 이름)
            blob_client = blob_service_client.get_blob_client(container=user, blob=blob.name)

            # 파일 다운로드 경로 => 위에서 선언한 local_path + 파일이름(blob.name)
            download_file_path = os.path.join(local_path, blob.name)
            # 그냥 무슨 파일 다운로드하는지 확인하는 출력문
            print("\nDownloading blob to \n\t" + download_file_path)

            # 파일 생성
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            gyro_list.append(blob.name)
            
        elif acc in blob.name and blob.name not in acc_list:
            blob_client = blob_service_client.get_blob_client(container=user, blob=blob.name)
            download_file_path = os.path.join(local_path, blob.name)
            print("\nDownloading blob to \n\t" + download_file_path)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            gyro_list.append(blob.name)

except Exception as ex:
    print('Exception:')
    print(ex)



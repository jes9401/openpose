import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import glob
import time
import pandas as pd
import datetime

local_path='./accgy/'
all_path = './all/'
kinect_path = './kinect/'
result_path = './result/'

gyro_list = glob.glob(os.path.join(local_path, '*GYROSCOPE.csv'))
gyro_list = list(map(lambda x:x.split('\\')[-1],gyro_list))

acc_list = glob.glob(os.path.join(local_path, '*ACCELEROMETER.csv'))
acc_list = list(map(lambda x:x.split('\\')[-1],acc_list))

all_file_list = glob.glob(os.path.join(all_path,'*ALL.csv'))
all_file_list = list(map(lambda x:x.split('\\')[-1],all_file_list))

kinect_list = glob.glob(os.path.join(kinect_path,'*KINECT.csv'))
kinect_list = list(map(lambda x:x.split('\\')[-1],kinect_list))

result_list = glob.glob(os.path.join(result_path,'*RESULT.csv'))
result_list = list(map(lambda x:x.split('\\')[-1],result_list))

while True:
    try:
        # 연결 문자열
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')


        user = 'jes'
        
        # 연결하는 부분입니다. 
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(user)
        blob_service_client2 = BlobServiceClient.from_connection_string(connect_str)
        container_client2 = blob_service_client.get_container_client('kinect-test')
        blob_service_client3 = BlobServiceClient.from_connection_string(connect_str)
        container_client3 = blob_service_client.get_container_client('result')
        
    except Exception as ex:
        print('Exception:')
        print(ex)
        continue
    blob_list = container_client.list_blobs()
    blob_list_kinect = container_client2.list_blobs()
    blob_list_result = container_client3.list_blobs()
    
    gyro = 'GYROSCOPE.txt'
    acc = 'ACCELEROMETER.txt'
    k = 'KINECT.csv'
        
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
                
        if acc in blob.name and blob.name[:-3]+"csv" not in acc_list:
            blob_client = blob_service_client.get_blob_client(container=user, blob=blob.name)
            download_file_path = os.path.join(local_path, blob.name[:-3]+"csv")
            print("\nDownloading blob to \n\t" + download_file_path)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            acc_list.append(blob.name[:-3]+"csv")
            
    for blob in blob_list_kinect:
        if k in blob.name and blob.name[:-3]+"csv" not in kinect_list:
            blob_client = blob_service_client2.get_blob_client(container='kinect-test', blob=blob.name)
            
            download_file_path = os.path.join(kinect_path, blob.name[:-3]+"csv")
            # 그냥 무슨 파일 다운로드하는지 확인하는 출력문
            print("\nDownloading blob to \n\t" + download_file_path)

            # 파일 생성
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            kinect_list.append(blob.name[:-3]+"csv") 

    blob_name_list = list(map(lambda x:x.name,blob_list))
    blob_name_list2 = list(map(lambda x:x.name,blob_list_kinect))
    blob_name_list3 = list(map(lambda x:x.name,blob_list_result))
    
    for acc_file in acc_list:
            temp = acc_file[:-17]
            acc_temp = acc_file
            gyro_temp = temp+"GYROSCOPE.csv"
            if temp+"GYROSCOPE.csv" in gyro_list and temp+"ALL.csv" not in all_file_list:
                df_acc = pd.read_csv(local_path+acc_temp)
                df_gyro = pd.read_csv(local_path+gyro_temp)
                time_list = list(df_acc['Time'])
                #df_acc['Time'] =list(map(lambda x:"".join(x.split(':')[:-1])+x.split(':')[-1][:2],time_list))
                #df_acc['Time'] = list(map(lambda x:x.replace(x.split(':')[-1],x.split(':')[-1][:2]),time_list))
                for x in range(len(time_list)):
                    time_temp = time_list[x]
                    rf = time_temp.rfind(":")
                    first = time_temp[:rf+1]
                    last = time_temp[rf+1:][:2]
                    time_list[x] = first+last
                df_acc['Time'] = time_list
                
                df_acc.drop('TimeZone',axis=1,inplace=True)
                df_acc.drop_duplicates(['Time'],inplace=True)
                df_acc.rename(columns={'X':'acc_x'},inplace=True)
                df_acc.rename(columns={'Y':'acc_y'},inplace=True)
                df_acc.rename(columns={'Z':'acc_z'},inplace=True)

                time_list = list(df_gyro['Time'])
                #df_gyro['Time'] =list(map(lambda x:"".join(x.split(':')[:-1])+x.split(':')[-1][:2],time_list))
                #df_gyro['Time'] = list(map(lambda x:x.replace(x.split(':')[-1],x.split(':')[-1][:2]),time_list))
        
                for x in range(len(time_list)):
                    time_temp = time_list[x]
                    rf = time_temp.rfind(":")
                    first = time_temp[:rf+1]
                    last = time_temp[rf+1:][:2]
                    time_list[x] = first+last
                df_gyro['Time'] = time_list
                
                df_gyro.drop('TimeZone',axis=1,inplace=True)
                df_gyro.drop_duplicates(['Time'],inplace=True)
                df_gyro.rename(columns={'X':'gyro_x'},inplace=True)
                df_gyro.rename(columns={'Y':'gyro_y'},inplace=True)
                df_gyro.rename(columns={'Z':'gyro_z'},inplace=True)
                
                df_all = pd.merge(df_acc,df_gyro,on='Time')
                time_list = list(df_all['Time'])
                df_all['Time'] = list(map(lambda x:x.replace(x.split(':')[-1],x.split(':')[-1][:1]),time_list))
                
                time_list = list(df_all['Time'])
                for x in range(len(time_list)):
                    time_temp = time_list[x]
                    rf = time_temp.rfind(":")
                    first = time_temp[:rf+1]
                    last = time_temp[rf+1:][:1]
                    time_list[x] = first+last
                df_all['Time'] = time_list
                
                df_all.drop_duplicates(['Time'],inplace=True)
    
                df_all.to_csv(all_path+temp+"ALL.csv",index=False)
                
                print("create",temp+"ALL.csv")

                file_name = temp+"ALL.csv"
                all_file_list.append(file_name)

     
                
    for kinect_file in kinect_list:
        temp2 = kinect_file.replace('KINECT','RESULT')
        if temp2 in blob_name_list3:
            continue
        
        column_list = ['id','Time']
        count = 0


        for i in range(32):
            column_list.extend(["3d_x"+str(i),"3d_y"+str(i),"3d_z"+str(i)])

        df_real = pd.DataFrame(columns=column_list)
        same_list = []
        kinect_time = kinect_file.split("_")[0]
        if kinect_time[-2:]=="59":
            p_time = str(int(kinect_time)-1)
            f_time = kinect_time[:-4]+str(int(kinect_time[-4:-2])+1)+"00"
        elif kinect_time[-2:]=="00":
            p_time = kinect_time[:-4]+str(int(kinect_time[-4:-2])-1)+"59"
            f_time = str(int(kinect_time)+1)
        else:
            p_time = str(int(kinect_time)-1)
            f_time = str(int(kinect_time)+1)
        
        for x in all_file_list:
            if kinect_time in x:
                same_list.append(x)
            if p_time in x:
                same_list.append(x)
            #if f_time in x:
            #    same_list.append(x)

        if len(same_list)==2:
            for s in range(len(same_list)):
                if s==0:
                    df_all = pd.read_csv(all_path+same_list[s])
                else:
                    df_all2 = pd.read_csv(all_path+same_list[s])
                    df_all = pd.concat([df_all, df_all2], axis = 0)
        else:
            continue
        
        kinect = pd.read_csv(kinect_path+kinect_file)
        size = len(kinect)
        size = size - (size%32)
        kinect = kinect.iloc[:size,:]
        num = kinect['frame_num'].unique()
        for i in range(len(num)):
            temp = kinect['frame_num'] == num[i]
            df_temp = kinect[temp]
            # 조인트 별로  TIme이 다르네..? 0번 조인트 기준으로 묶을게요
            row_list = [str(i+1),df_temp['Time'][32*i]]
            
            for index,row in df_temp.iterrows():
                n = index+1
                # 3d 좌표
                row_list.extend([row[6],row[7],row[8]])
            df_real.loc[count] = row_list
            count+=1
        time_list = list(df_real['Time'])
        for i in range(len(time_list)):
    #         if time_list[i].count(":")!=3:
            if time_list[i].split(":")[-1]=="0":
                time_list[i] = time_list[i][:-1]+"0"
        time_list = list(map(lambda x:str(datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S:%f")),time_list))
        
        # 소수점
#         time_list = list(map(lambda x:x.replace("."+x.split(".")[-1],":"+x.split(".")[-1][:2]),time_list))
        for x in range(len(time_list)):
            time_temp = time_list[x]
            rf = time_temp.rfind(".")
            first = time_temp[:rf]
            last = time_temp[rf+1:][:1]
            time_list[x] = first+":"+last
        df_real['Time'] = time_list

        df_merge = pd.merge(df_real,df_all,on="Time")
        df_merge.drop_duplicates(['Time'],inplace=True)  
        
        kinect_file_name = kinect_time+'_RESULT.csv'
        
        time_list = list(df_merge['Time'])
        for x in range(len(time_list)):
            time_temp = time_list[x]
            rf = time_temp.rfind(":")
            first = time_temp[:rf]
            last = time_temp[rf+1:]
            time_list[x] = first+"."+last
        df_merge['Time'] = time_list
        df_merge.to_csv('./result/'+kinect_file_name,index=False)
        
        print("create",kinect_file_name)
        result_list.append(kinect_file_name)

        
        if kinect_file_name not in blob_name_list3:
            upload_file_path = os.path.join(result_path,kinect_file_name)
            blob_client = blob_service_client3.get_blob_client(container='result', blob=kinect_file_name)
            with open(upload_file_path,"rb") as data:
                blob_client.upload_blob(data,overwrite=True)
            print("*"*100)
            print(kinect_file_name,'upload')
    time.sleep(3)

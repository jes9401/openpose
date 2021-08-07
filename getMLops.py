import requests
import time
import datetime
import json
import pymongo
import pandas as pd

# 데이터의 인덱스를 저장할 리스트
index_list = []
MONGO_HOST = '아이피 넣기'
MONGO_PORT = '포트 넣기'
myclient = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
db = myclient['test']
collection = db['result']

while True:   
    # 아까 위에서 말한 url에서 데이터를 읽어옴
    get_data = requests.get('주소넣기')
    # json 
    data = get_data.json()
    
    
    for i in data:
        # 인덱스가 index_list에 없는 경우에만 동작을 수행
        if i not in index_list:
            # 시작 시간
            data_time = data[i]['start_time']
            # motion 값들은 a_standard 이런 식으로 넘어오는데
            # 어떻게 사용할지 모르겠어서 일단 replace 함수 써서 standard << 이런식으로 저장함
            a_motion = data[i]['a_motion'].replace("a_",'')
            v_motion = data[i]['v_motion'].replace("v_",'')
            r_motion = data[i]['r_motion'].replace("r_",'')
            # 무슨 운동을 했는지 저장한 변수 => TwoHand0, TwoHand1, TwoHand2 
            exercise = data[i]['exercise']
            
            # 받아온 값중에 None 값이 하나라도 있으면 처음으로 돌아감
            if data_time == None or a_motion == None or v_motion == None or r_motion == None or exercise ==None:
                time.sleep(5)
                continue
            else:
                # 단순히 값 잘 받아왔는지 확인하기 위한 출력문
                print(data_time,a_motion,v_motion,r_motion,exercise)
                
                # 시작 시간 자체는 문자열로 받아오기 때문에 Z 만 더해주면
                # find 내에 조건으로 바로 사용할 수 있다
                start =data_time+"Z"
                # 그런데 1분은 더해주려면 datetime 형태로 변환하고
                # 1분을 더한 뒤 다시 문자열로 변환해야함
                end = datetime.datetime.strptime(start, '%Y-%m-%dT%H:%M:%S.%fZ')
                end += datetime.timedelta(minutes=1)
                end = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                print("첫번째 = ",start,end)
                # print(end)를 해보면 알겠지만 ms 부분이 6자리로 나올 경우가 있음
                # 그래서 .을 기준으로 나눴을때 제일 마지막 원소의 크기가 4가 아니면
                # ex) 2021-08-06T23:28:31.065000Z 
                #     => 2021-08-06T23:28:31 + 065 + Z   로 변경
                if len(end.split(".")[-1])!=4:
                    end = end[:end.rfind(".")+1]+end.split(".")[-1][:3]+"Z"
                print("두번째 = ",start,end)
                # 시작, 끝 시간을 이용해서 데이터 조회
                res = collection.find({'Time':{'$gt':start,'$lt':end}})
                
                # 그냥 데이터 프레임으로 변환해봤음 
                df_res = pd.DataFrame(res)
                
                
                ################################################
                # 이 else문 안에서 모델 재학습 진행하면 될 듯?
                ################################################
                
            # 동작을 완료한 뒤 index_list에 해당 index를 추가해준다
            index_list.append(i)
        else:
            pass
    
    # 임의로 일단 5초씩 딜레이를 줬는데 
    # 돌리다가 이유 모르겠을 에러가 뜨면 시간을 좀 더 늘려보기를 바람.
    time.sleep(5)

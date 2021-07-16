column_list = ['Time','frame_num']
count = 0


for i in range(32):
    column_list.extend(["3d_x"+str(i),"3d_y"+str(i),"3d_z"+str(i)])
    
df_real = pd.DataFrame(columns=column_list)

for kinect_file in kinect_list:
    same_list = []
    kinect_time = kinect_file.split("_")[0]
    p_time = str(int(kinect_time)-1)
    f_time = str(int(kinect_time)+1)
    for x in all_file_list:
        if kinect_time in x:
            same_list.append(x)
        if p_time in x:
            same_list.append(x)
        if f_time in x:
            same_list.append(x)
    
    if len(same_list)!=0:
        for s in range(len(same_list)):
            if s==0:
                df_all = pd.read_csv(all_path+same_list[s])
            else:
                df_all2 = pd.read_csv(all_path+same_list[s])
                df_all = pd.concat([df_all, df_all2], axis = 0)
    
    kinect = pd.read_csv(kinect_path+kinect_file)
    num = kinect['frame_num'].unique()
    for i in range(len(num)):
        temp = kinect['frame_num'] == i+1
        df_temp = kinect[temp]
        # 조인트 별로  TIme이 다르네..? 0번 조인트 기준으로 묶을게요
        row_list = [df_temp['Time'][32*i],str(i+1)]
        for index,row in df_temp.iterrows():
            n = index+1
            # 3d 좌표
            row_list.extend([row[6],row[7],row[8]])
        try:
            df_real.loc[count] = row_list
            count+=1
        except Exception as e:
            print(e)
    time_list = list(df_real['Time'])
    for i in range(len(time_list)):
        if time_list[i].count(":")!=3:
            time_list[i] = time_list[i]+":00"
    time_list = list(map(lambda x:str(datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S:%f")),time_list))
    # 소수점
    time_list = list(map(lambda x:x.replace("."+x.split(".")[-1],":"+x.split(".")[-1][:1]),time_list))

    df_real['Time'] = time_list

    df_merge = pd.merge(df_real,df_all,on="Time")
    df_merge.drop_duplicates(['Time'],inplace=True)
    df_merge.to_csv('./result/'+kinect_time+'_RESULT.csv',index=False)
    
    

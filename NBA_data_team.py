from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
from selenium import webdriver
import pymysql
from sqlalchemy import create_engine
import os
os.chdir("C:\\Users\\james\\Desktop\\NBA")

# MySQL資料庫設定
db_settings = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": '''password''',
    "charset":"utf8"
}
try:
    # 建立Connection物件
    conn = pymysql.connect(**db_settings)
        
    with conn.cursor() as cursor:
    
        command = 'create database nbadb'
        cursor.execute(command)
        conn.commit()
        conn.close()

except Exception as ex:
    print(ex)
    

#找出欄位 sort_title
url1 = 'https://tw.global.nba.com/statistics/teamstats/'
driver = webdriver.Chrome("chromedriver.exe")
driver.get(url1)
soup = BeautifulSoup(driver.page_source)
driver.close()

sort_lst = soup.find_all('th')

sort_title = []
for i in sort_lst:
    sort_title.append(i.text)
sort_title = sort_title[1:4]+sort_title[15:17]+sort_title[4:5]+sort_title[17:19]+sort_title[5:6]+sort_title[19:21]+sort_title[6:15]
sort_title.append('賽季')
sort_title.append('賽制')
sort_title.append('城市')
sort_title.append('州')

#完整程式碼
season = []
game = [2,4] #分析網頁後賽制的網址代碼
seasontype = ['RegularSeason','Playoffs'] #賽制
for i in range(2017,2022): #分析網頁後賽季的網址代碼，可自由抓取所需年份
    season.append(i)

df_final = pd.DataFrame()
for game_num in range (len(game)):
    for season_num in range (len(season)):
        url = 'https://tw.global.nba.com/stats2/league/teamstats.json?conference=All&division=All&locale=us&season='+str(season[season_num])+'&seasonType='+str(game[game_num])
        data = requests.get(url).text
        data_json = json.loads(data)
        
        #球隊
        data_json['payload']['teams'][0]['profile']['abbr']
        
        #賽季
        data_json['payload']['season']['yearDisplay']

        #城市
        data_json['payload']['teams'][0]['profile']['city']

        #州 因爬蟲的資料內沒有此欄資料，自行製作對照表
        df_state_surface = pd.read_csv('state_surface.csv')

        #其餘所需資料 sort_data
        #將sort_data內容依所需index排列 sort_num
        #sort_data[sort_num] = sort_value
        sort_data = list(data_json['payload']['teams'][0]['statAverage'])
        
        sort_num = [11,6,5,4,20,19,18,10,9,8,14,2,15,16,0,21,17,1,7]
        sort_value = []
        for i in range (len(sort_num)):
            sort_value.append(sort_data[sort_num[i]])
        
        #合併所有資料至 final_data  
        final_data = []
        for i in range(len(data_json['payload']['teams'])): #30
            temp_data = []
            temp_data.append(data_json['payload']['teams'][i]['profile']['abbr']) #球隊
            for k in sort_value: #
                temp_data.append(data_json['payload']['teams'][i]['statAverage'][k]) #19
            temp_data.append(data_json['payload']['season']['yearDisplay']) #賽季期間
            temp_data.append(seasontype[game_num]) #賽制
            temp_data.append(data_json['payload']['teams'][i]['profile']['city']) #城市
            if data_json['payload']['teams'][i]['profile']['city'] in list(df_state_surface["城市"]): #州
                index_num = list(df_state_surface["城市"]).index(data_json['payload']['teams'][i]['profile']['city'])
                temp_data.append(df_state_surface["州"][index_num])
            final_data.append(temp_data)

        #創立一個DataFrame，欄位為sort_title
        df = pd.DataFrame(columns = sort_title)
        
        #將final_data內的資料依序放入DataFrame中
        for i in range(len(final_data)):
            df.loc[i] = final_data[i]
        
        #將DataFrame 存至本機端
        df.to_csv('datafile/'+data_json['payload']['season']['yearDisplay']+seasontype[game_num]+'_team.csv',encoding ='utf_8_sig',index = False)

        df_final = pd.concat([df_final,df])

        #建立MySQL所需欄位資訊
        db_title = ['team']
        for i in sort_value:
            db_title.append(i)
        db_title.append('season')
        db_title.append('seasonType')
        db_title.append('city')
        db_title.append('state')
        
        #MySQL 欄位資訊 dtype <- db_type
        char = 'VARCHAR(255)'
        int_num = 'INT(255)'
        float_num = 'DECIMAL(10,2)'
        
        db_type = []
        
        for i in final_data[0]:
            if type(i) == str:
                db_type.append(char)
            elif type(i) == int:
                db_type.append(int_num)
            else:
                db_type.append(float_num)
        
        #MySQL 欄位名稱+欄位資訊 db_list
        db_list = []
        
        for i in range(len(final_data[0])):
            db_list.append(db_title[i]+' '+db_type[i])
        
        #將欄位名稱換成英文
        df.columns = db_title 
        
        #連線至MySQL 創建資料表並建立欄位資訊
        db_settings = {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "password": '''password''',
            "database": "nbadb",
            "charset": "utf8"
        }
        try:
            conn = pymysql.connect(**db_settings)
                
            with conn.cursor() as cursor:
            
                command = 'create table '+str(season[season_num])+str(season[season_num]+1)+seasontype[game_num]+'_team (' + str(db_list).replace('[','').replace(']','').replace("'",'') + ')'
                cursor.execute(command)
                # for i in cursor:
                #     print(i)
                conn.commit()

        except Exception as ex:
            print(ex)   
        
        #連線至MySQL 將DataFrame放進創建好的資料表內
        engine = create_engine('mysql+pymysql://root:'+'''password'''+'@localhost:3306/nbadb?charset=utf8')
        engine.connect()
        df.to_sql(str(season[season_num])+str(season[season_num]+1)+seasontype[game_num].lower()+"_team", engine, index=False, if_exists = 'replace')
        engine.dispose()

#將所有資料存至本地端
df_final.to_csv('datafile/'+str(season[0])+'-'+str(season[-1]+1)+'All_team.csv',encoding ='utf_8_sig',index = False)

#將欄位名稱換成英文
df_final.columns = db_title

#連線至MySQL 創建資料表並建立欄位資訊
db_settings = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": '''password''',
    "database": "nbadb",
    "charset": "utf8"
}
try:
    conn = pymysql.connect(**db_settings)
        
    with conn.cursor() as cursor:
    
        command = 'create table '+str(season[0])+str(season[-1]+1)+'All_team (' + str(db_list).replace('[','').replace(']','').replace("'",'') + ')'
        cursor.execute(command)
        # for i in cursor:
        #     print(i)
        conn.commit()

except Exception as ex:
    print(ex)   

#連線至MySQL 將所有資料的DataFrame放進創建好的資料表內
engine = create_engine('mysql+pymysql://root:'+'''password'''+'@localhost:3306/nbadb?charset=utf8')
engine.connect()
df_final.to_sql(str(season[0])+str(season[-1]+1)+'All_team'.lower(), engine, index=False, if_exists = 'replace')
engine.dispose()
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
url1 = 'https://tw.global.nba.com/statistics/'
driver = webdriver.Chrome("chromedriver.exe")
driver.get(url1)
soup = BeautifulSoup(driver.page_source)
driver.close()

sort_lst = soup.find_all('th')

sort_title = []
for i in sort_lst:
    sort_title.append(i.text)
sort_title = sort_title[0:11]+sort_title[24:26]+sort_title[11:12]+sort_title[26:28]+sort_title[12:13]+sort_title[28:30]+sort_title[13:19]
sort_title.append('賽季')
sort_title.append('賽制')
sort_title.append('主要位置')
sort_title.append('次要位置')
sort_title.append('城市')
sort_title.append('州')

#完整程式碼
season = []
game = [2,4] #分析網頁後賽制的網址代碼
seasontype = ['RegularSeason','Playoffs'] #賽季類型
for i in range(2017,2022): #分析網頁後賽季的網址代碼，可自由抓取所需年份
    season.append(i)

df_final = pd.DataFrame()
for game_num in range (len(game)):
    for season_num in range (len(season)):
        n=0
        temp_final = []
        while True:
            url = 'https://tw.global.nba.com/stats2/league/playerstats.json?conference=All&country=All&individual=All&locale=us&pageIndex=' + str(n) + '&position=All&qualified=false&season='+str(season[season_num])+'&seasonType='+str(game[game_num])+'&split=All+Team&statType=points&team=All&total=perGame'
            data = requests.get(url).text
            data_json = json.loads(data)
            
            #排名
            data_json['payload']['players'][0]['rank']
            
            #球員
            data_json['payload']['players'][0]["playerProfile"]["displayName"]
            
            #球隊
            data_json['payload']['players'][0]["teamProfile"]["abbr"]
            
            #賽季
            data_json['payload']['season']['yearDisplay']
            
            #位置
            data_json['payload']['players'][0]["playerProfile"]["position"]
            
            #城市
            data_json['payload']['players'][0]['teamProfile']['city']

            #州 因爬蟲的資料內沒有此欄資料，自行製作對照表
            df_state_surface = pd.read_csv('state_surface.csv')

            #其餘所需資料 sort_data
            #將sort_data內容依所需index排列 sort_num
            #sort_data[sort_num] = sort_value
            sort_data = list(data_json['payload']['players'][0]['statAverage'])
            
            sort_num = [11,12,15,16,0,13,3,6,5,4,20,19,18,10,9,8,14,2,17,1,21,7]
            sort_value = []
            for i in range (len(sort_num)):
                sort_value.append(sort_data[sort_num[i]])
            
            #合併所有資料至 final_data  
            sort_final = []
            temp_data = []
            for i in range(len(data_json['payload']['players'])): #50
                sort_final.append(data_json['payload']['players'][i]['rank'])
                sort_final.append(data_json['payload']['players'][i]["playerProfile"]["displayName"])
                sort_final.append(data_json['payload']['players'][i]["teamProfile"]["abbr"])
                for k in sort_value: #22
                    sort_final.append(data_json['payload']['players'][i]['statAverage'][k]) #22
                sort_final.append(data_json['payload']['season']['yearDisplay']) #賽季期間
                sort_final.append(seasontype[game_num]) #賽制
                if len(data_json['payload']['players'][i]["playerProfile"]["position"]) > 1:  #位置 G、F-G、PG
                    try:
                        sort_final.append(data_json['payload']['players'][i]["playerProfile"]["position"].split("-")[0])
                        sort_final.append(data_json['payload']['players'][i]["playerProfile"]["position"].split("-")[1])
                    except:
                        sort_final.append(data_json['payload']['players'][i]["playerProfile"]["position"])
                else: 
                    sort_final.append(data_json['payload']['players'][i]["playerProfile"]["position"])
                    sort_final.append(data_json['payload']['players'][i]["playerProfile"]["position"])
                sort_final.append(data_json['payload']['players'][i]['teamProfile']['city']) #城市
                if data_json['payload']['players'][i]["teamProfile"]["city"] in list(df_state_surface["城市"]): #州
                    index_num = list(df_state_surface["城市"]).index(data_json['payload']['players'][i]["teamProfile"]["city"])
                    sort_final.append(df_state_surface["州"][index_num])
                temp_data.append(sort_final) 
                sort_final=[]
                
            #將while迴圈內每次收集完的temp_data存至temp_final串列內
            temp_final.append(temp_data) 
            
            n += 1
            if len(data_json['payload']['players']) < 50: #每頁資料為50筆，如<50為最後一頁資料
                break
        
        #創立一個DataFrame，欄位為sort_title
        df = pd.DataFrame(columns = sort_title)
        
        #因為temp_final內每迴圈一次，index就會多一個串列，將temp_final內的每一筆資料都提出到final_data
        final_data = [] 
        for i in range(len(temp_final)):
            for j in temp_final[i]:
              final_data.append(j) 
        
        #將final_data內的資料依序放入DataFrame中
        for i in range(len(final_data)):
            df.loc[i] = final_data[i]     

        #將DataFrame 存至本機端
        df.to_csv('datafile/'+data_json['payload']['season']['yearDisplay']+seasontype[game_num]+'.csv',encoding ='utf_8_sig',index = False)

        df_final = pd.concat([df_final,df])

        #建立MySQL所需欄位資訊
        db_title = ['rankid','player','team']
        for i in sort_value:
            db_title.append(i)
        db_title.append('season')
        db_title.append('seasonType')
        db_title.append('main_position')
        db_title.append('second_position')
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
        
        #將df欄位名稱換成英文
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
            
                command = 'create table '+str(season[season_num])+str(season[season_num]+1)+seasontype[game_num]+' (' + str(db_list).replace('[','').replace(']','').replace("'",'') + ')'
                cursor.execute(command)
                # for i in cursor:
                #     print(i)
                conn.commit()

        except Exception as ex:
            print(ex)   
        
        #連線至MySQL 將DataFrame放進創建好的資料表內
        engine = create_engine('mysql+pymysql://root:'+'''password'''+'@localhost:3306/nbadb?charset=utf8')
        engine.connect()
        df.to_sql(str(season[season_num])+str(season[season_num]+1)+seasontype[game_num].lower(), engine, index=False, if_exists = 'replace')
        engine.dispose()

#之前匯入資料時發現只有此欄位資料值為PG，更改為G
df_final.reset_index(inplace=True, drop=True)
df_final["主要位置"][415] = "G"
df_final["次要位置"][415] = "G"
df_final.to_csv('datafile/'+str(season[0])+'-'+str(season[-1]+1)+'All.csv',encoding ='utf_8_sig',index = False)

#製作索引資料表，並存放至MySQL資料庫
dic1 = {"賽季":list(df_final["賽季"].unique())}
df_season = pd.DataFrame(dic1)
df_season.to_csv("datafile/season_table.csv",encoding ='utf_8_sig',index = False)
df_season.columns = ["season"]

dic2 = {"賽制":list(df_final["賽制"].unique())}
df_seasontype = pd.DataFrame(dic2)
df_seasontype.to_csv("datafile/seasontype_table.csv",encoding ='utf_8_sig',index = False)
df_seasontype.columns = ["seasontype"]

dic3 = {"球隊":list(df_final["球隊"].unique())}
df_team = pd.DataFrame(dic3)
df_team.to_csv("datafile/team_table.csv",encoding ='utf_8_sig',index = False)
df_team.columns = ["team"]

dic4 = {"城市":list(df_final["城市"].unique())}
df_city = pd.DataFrame(dic4)
df_city.to_csv("datafile/city_table.csv",encoding ='utf_8_sig',index = False)
df_city.columns = ["city"]

dic5 = {"州":list(df_final["州"].unique())}
df_state = pd.DataFrame(dic5)
df_state.to_csv("datafile/state_table.csv",encoding ='utf_8_sig',index = False)
df_state.columns = ["state"]

#將df_final欄位名稱換成英文
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
    
        command = 'create table '+str(season[0])+str(season[-1]+1)+'All (' + str(db_list).replace('[','').replace(']','').replace("'",'') + ')'
        cursor.execute(command)
        # for i in cursor:
        #     print(i)
        conn.commit()

except Exception as ex:
    print(ex)   

#連線至MySQL 將所有資料的DataFrame放進創建好的資料表內
engine = create_engine('mysql+pymysql://root:'+'''password'''+'@localhost:3306/nbadb?charset=utf8')
engine.connect()
df_season.to_sql(str(season[0])+str(season[-1]+1)+'season_table', engine, index=False, if_exists = 'replace')
df_seasontype.to_sql(str(season[0])+str(season[-1]+1)+'seasontype_table', engine, index=False, if_exists = 'replace')
df_team.to_sql(str(season[0])+str(season[-1]+1)+'team_table', engine, index=False, if_exists = 'replace')
df_city.to_sql(str(season[0])+str(season[-1]+1)+'city_table', engine, index=False, if_exists = 'replace')
df_state.to_sql(str(season[0])+str(season[-1]+1)+'state_table', engine, index=False, if_exists = 'replace')
df_final.to_sql(str(season[0])+str(season[-1]+1)+'All'.lower(), engine, index=False, if_exists = 'replace')

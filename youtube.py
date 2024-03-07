from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


### API connection

def ApiConnect():
    api_ID = "AIzaSyBDVNvvTIxEk90R0rPat4lGTRrJDmCk-e4"
    api_service_name ="Youtube"
    api_version_name = "v3"
    youtube = build(api_service_name, api_version_name,developerKey=api_ID )
    return youtube

youtube = ApiConnect()

### get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
    part = "snippet,ContentDetails,statistics",
    id = channel_id 
    )

    response = request.execute()

    for i in response['items']:
        data=dict(Channel_Name = i["snippet"]["title"],
                Channel_ID = i["id"],#here channel id is the column in the table we are creating from channel info
                Views=i["statistics"]["viewCount"],
                Subscribers = i["statistics"]["subscriberCount"],
                Total_videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])##storing the extracted data in dictionary format --mongodb only stores data in json format
    return data

#get video ids
def get_video_ids(channel_id):
    video_ids= []
    response = youtube.channels().list(id=channel_id,
                                    part = 'contentDetails').execute()
    Playlist_Id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    next_page_token = None
    while True:
        response1=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken = next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token= response1.get('nextPageToken')
        if next_page_token is None:
            break

    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]

    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for  item in response['items']:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Video_Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item.get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    FavoriteCount=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)

    return video_data
        
#get comment inormation
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass

    return Comment_data

#get_playlist_details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    PublishedAt=item['snippet']['publishedAt'],
                    Videocount=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_data

#upload to mongodb

client=pymongo.MongoClient("mongodb+srv://yuva0398:yuva@cluster0.jwuol0q.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details= get_channel_info(channel_id)
    vi_ids= get_video_ids(channel_id)
    vi_details= get_video_info(vi_ids)
    Com_details= get_comment_info(vi_ids)
    pl_details = get_playlist_details(channel_id)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details, "playlist_information":pl_details, 
                      "video_information":vi_details, "comment_information":Com_details})
    
    return "upload completed successfully"

#Table creation for channels,playlist,videos,comments
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                            Channel_ID varchar(80) primary key,
                            Subscribers bigint,
                            Views bigint,
                            Total_videos int,
                            Channel_Description text,
                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channel table already created")


    ch_list=[]
    db= client["Youtube_data"] #calling db in postgresql
    coll1=db["channel_details"] #calling db in mongo db
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):   #creating list to store channel details from mongodb...empty{} braces for selecting all the column from channel details
        ch_list.append(ch_data["channel_information"])                   #selecting _id =0 to negate the id value from mongodb 

    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():#should have 2 variable for iteration, column name display like key ,row value as value  -->#print(index,row) #postgres column name
        insert_query='''insert into channels(Channel_Name,   
                                            Channel_ID,
                                            Subscribers,
                                            Views,
                                            Total_videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_Name'], #name are similar to df column name
                row['Channel_ID'],
                row['Subscribers'],
                row['Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("Channel values are already inserted")

#creating playlist table in postgresql
def playlist_table():
    mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="12345",
        database="youtube_data",
        port=5432)
    cursor=mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Videocount int)'''

    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
    #print(len(pl_data["playlist_information"])) -->this provide no of records in each playlist
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():#should have 2 variable for iteration, column name display like key ,row value as value  -->#print(index,row) #postgres column name 
        insert_query='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Videocount)
                                                
                                            values(%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Playlist_Id'],
                row['Title'], #name are similar to df column name
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Videocount'],
                )
        

        cursor.execute(insert_query,values)
        mydb.commit()

    #pl_data["playlist_information"][0]



#create video table in postgresql
def video_table():

    mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="12345",
        database="youtube_data",
        port=5432)
    cursor=mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Video_Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    FavoriteCount int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(10))'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
    #print(len(pl_data["playlist_information"])) -->this provide no of records in each playlist
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Video_Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            FavoriteCount,
                                            Definition,
                                            Caption_Status)
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['channel_Name'],
                row['channel_Id'], 
                row['Video_Id'],
                row['Video_Title'],
                row['Tags'],
                row['Thumnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'], 
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['FavoriteCount'],
                row['Definition'],
                row['Caption_Status']
                )
    

        cursor.execute(insert_query,values)
        mydb.commit()


#comment table creation in postgre

def comments_table():
    mydb=psycopg2.connect(
            host="localhost",
            user="postgres",
            password="12345",
            database="youtube_data",
            port=5432)
    cursor=mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                    Video_Id varchar(100),
                                                    Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_Published timestamp
                                                    )'''

    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
    #print(len(pl_data["playlist_information"])) -->this provide no of records in each playlist
            for i in range(len(com_data["comment_information"])):
                    com_list.append(com_data["comment_information"][i])

    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
                                            )
                                                    values(%s,%s,%s,%s,%s)'''
            
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published'])


            cursor.execute(insert_query,values)
            mydb.commit()

def tables():
    channels_table()
    playlist_table()
    video_table()
    comments_table()
    
    return "tables created successfully"

def show_channel_table():
    ch_list=[]
    db= client["Youtube_data"] #calling db in postgresql
    coll1=db["channel_details"] #calling db in mongo db
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):   
        ch_list.append(ch_data["channel_information"])                   #selecting _id =0 to negate the id value from mongodb 

    df=st.dataframe(ch_list)
    #print("channel_table",df)
    return df

def show_playlist_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
    #print(len(pl_data["playlist_information"])) -->this provide no of records in each playlist
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=st.dataframe(pl_list)
    print("playlist",df1)
    return df1

def show_video_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
    #print(len(pl_data["playlist_information"])) -->this provide no of records in each playlist
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    
    df2=st.dataframe(vi_list)
    
    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
    #print(len(pl_data["playlist_information"])) -->this provide no of records in each playlist
            for i in range(len(com_data["comment_information"])):
                    com_list.append(com_data["comment_information"][i])

    df3=st.dataframe(com_list)

    return df3

#stream lit

with st.sidebar:
    st.title(":Maroon[Youtube Data Harvesting and Warehousing]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")


if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_ID"])

 
    if channel_id in ch_ids:
        st.success("channel details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("Select the table for View",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    show_channel_table()
elif show_table=="Playlists":
    show_playlist_table()
elif show_table=="Videos":
    show_video_table()
elif show_table=="Comments":
    show_comments_table()

#SQL Connection

mydb=psycopg2.connect(
                host="localhost",
                user="postgres",
                password="12345",
                database="youtube_data",
                port=5432)
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and channel name",
                                              "2. Channels with most number of videos",
                                              "3. Top 10 most viewed videos",
                                              "4. Comments in each videos",
                                              "5. Videos with highest likes",
                                              "6. Likes of all videos",
                                              "7. Views of each channel",
                                              "8. Videos published in the year of 2022",
                                              "9. Average duration of all videos in each channel",
                                              "10. Videos with highest number of comments"))

if question=="1. All the videos and channel name":
    query1='''select video_title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit
    t1=cursor.fetchall()   #fetch the result which is stored in cursor and assigning it to a variable
    df=pd.DataFrame(t1,columns=["video title","channel name"]) #converting it into df and assigning column name 
    st.write(df)


elif question=="2. Channels with most number of videos":
    query2='''select channel_name as channelname, total_videos as total_no_of_videos from channels order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit
    t2=cursor.fetchall()  
    df2=pd.DataFrame(t2,columns=["channel name","total_no_of_Videos"]) 
    st.write(df2)

elif question=="3. Top 10 most viewed videos":
    query3='''select video_title as title,views as total_views,channel_name as channelname from videos 
            where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit
    t3=cursor.fetchall()  
    df3=pd.DataFrame(t3,columns=["video_title","views","channel name"])
    st.write(df3)

elif question=="4. Comments in each videos":
    query4='''select  video_title as title,comments as total_comments from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit
    t4=cursor.fetchall()   
    df4=pd.DataFrame(t4,columns=["video_title","comments"]) 
    st.write(df4)

elif question=="5. Videos with highest likes":
    query5='''select  video_title as title,likes as no_of_likes from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit
    t5=cursor.fetchall()   
    df5=pd.DataFrame(t5,columns=["video_title","likes"]) 
    st.write(df5)

elif question=="6. Likes of all videos":
    query6='''select  video_title as title,likes as no_of_likes from videos '''
    cursor.execute(query6)
    mydb.commit
    t6=cursor.fetchall()   
    df6=pd.DataFrame(t6,columns=["video_title","likes"]) 
    st.write(df6)

elif question=="7. Views of each channel":
    query7='''select  channel_name as channelname,views as view_count from channels '''
    cursor.execute(query7)
    mydb.commit
    t7=cursor.fetchall()   
    df7=pd.DataFrame(t7,columns=["channelname","viewcount"]) 
    st.write(df7)

elif question=="8. Videos published in the year of 2022":
    query8='''select  video_title as video_title,published_date as videorelease,channel_name as channelname from videos 
            where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit
    t8=cursor.fetchall()   
    df8=pd.DataFrame(t8,columns=["videotitle","videoreleasedate","channelname"]) 
    st.write(df8)

elif question=="9. Average duration of all videos in each channel":
    query9='''select  channel_name as channelname, avg(duration) as average_duration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit
    t9=cursor.fetchall()   
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"]) 
    #timestamp cannot be shown in streamlit so convert it into string format
    T9=[]
    for index,row in df9.iterrows(): #iter through df9
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)  #convert duration into string
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str)) #assigning column and values in dict format
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. Videos with highest number of comments":
    query10='''select  video_title as Video_title,channel_name as channelname,comments as comments_count from videos where comments is not null  order by comments desc'''
    cursor.execute(query10)
    mydb.commit
    t10=cursor.fetchall()   
    df10=pd.DataFrame(t10,columns=["video_title","channelname","comments_count"]) 
    st.write(df10)

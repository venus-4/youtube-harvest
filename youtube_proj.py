from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
#API key connection

def Api_connect():
    Api_Id="AIzaSyCMLqNTwyaFusvy0P5fZsifnh4x1IqqeJw"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data

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
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data

#get comment information
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

#upload to mongoDB

client=pymongo.MongoClient("mongodb+srv://suganya1746:mongodb@cluster0.vfmq7tw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"

#Table creation for channels,playlists,videos,comments
def channels_table(channel_name_r):
    
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Postsql1",
                        database="Youtube_Data",
                        port="5432")
        cursor=mydb.cursor()

        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    
        single_channel_det=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_r},{"_id":0}):
                single_channel_det.append(ch_data["channel_information"])
        df_single_channel_det=pd.DataFrame(single_channel_det)

        for index,row in df_single_channel_det.iterrows():
                insert_query='''insert into channels(Channel_Name ,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'])
        
        try:
                cursor.execute(insert_query,values)
                mydb.commit()
        except:
              news= f"Your Provided Channel Name {channel_name_r} is already exists"

              return news

        

                
#playlist table
def playlist_table(channel_name_r):
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="Postsql1",
                                database="Youtube_Data",
                                port="5432")
        cursor=mydb.cursor()

        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''

        cursor.execute(create_query)
        mydb.commit()

        single_playlist_det=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_r},{"_id":0}):
                single_playlist_det.append(ch_data["playlist_information"])
        df_single_playlist_det=pd.DataFrame(single_playlist_det[0])

        for index,row in df_single_playlist_det.iterrows():
                insert_query='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
                
                values=(row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['PublishedAt'],
                        row['Video_Count']
                        )

                
                cursor.execute(insert_query,values)
                mydb.commit()

#video table
def video_table(channel_name_r):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Postsql1",
                        database="Youtube_Data",
                        port="5432")
        cursor=mydb.cursor()

        

        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                Channel_Id varchar(100),
                                                Video_Id varchar(30) primary key,
                                                Title varchar(150),
                                                Tags text,
                                                Thumbnail varchar(200),
                                                Description text,
                                                Published_Date timestamp,
                                                Duration interval,
                                                Views bigint,
                                                Likes bigint,
                                                Comments int,
                                                Favorite_Count int,
                                                Definition varchar(10),
                                                Caption_Status varchar(50)
                                                        )'''

        cursor.execute(create_query)
        mydb.commit()
        single_video_det=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_r},{"_id":0}):
                single_video_det.append(ch_data["video_information"])
        df_single_video_det=pd.DataFrame(single_video_det[0])

        for index,row in df_single_video_det.iterrows():
                insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


                values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                        )


                cursor.execute(insert_query,values)
                mydb.commit()

#comments table
def comments_table(channel_name_r):
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="Postsql1",
                                database="Youtube_Data",
                                port="5432")
        cursor=mydb.cursor()

        create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                Video_Id varchar(50),
                                                                Comment_Text text,
                                                                Comment_Author varchar(150),
                                                                Comment_Published timestamp
                                                                )'''


        cursor.execute(create_query)
        mydb.commit()

        single_comment_det=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_r},{"_id":0}):
                single_comment_det.append(ch_data["comment_information"])
        df_single_comment_det=pd.DataFrame(single_comment_det[0])

        for index,row in df_single_comment_det.iterrows():
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
                        row['Comment_Published']
                        )
                                                        
                        
                        

                
                cursor.execute(insert_query,values)
                mydb.commit()

def tables(req_channel):
    
    news=channels_table(req_channel)
    if news:
          return news
    else:
        playlist_table(req_channel)
        video_table(req_channel)
        comments_table(req_channel)

        return "Tables created successfully"

def show_channel_table():
        ch_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data["channel_information"])
        df=st.dataframe(ch_list)

        return df
def show_playlists_table():
        pl_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        df1=st.dataframe(pl_list)

        return df1
def show_videos_table():
        vi_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
        df2=st.dataframe(vi_list)

        return df2
def show_comments_table():
        cd_list=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for cd_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(cd_data["comment_information"])):
                        cd_list.append(cd_data["comment_information"][i])
        df4=st.dataframe(cd_list)

        return df4

#streamlit code

with st.sidebar:
    st.title(":red[Youtube Data Harvesting and Warehousing]")
    st.header("Skill Takeaway")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDb")
    st.caption("API Integration")
    st.caption("Data Management using Mongodb and SQL")

channel_id=st.text_input("Enter the Channel_Id:")

if st.button("Collect and Store Data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append((ch_data["channel_information"]["Channel_Id"]))
    
    if channel_id in ch_ids:
        st.success("Channels Details already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)

all_channels=[]
db=client["Youtube_data"]
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        all_channels.append(ch_data["channel_information"]["Channel_Name"])

unique_channel=st.selectbox("Select the Channel:",all_channels)

if st.button("Migrate to SQL"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("Select Table Name",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    show_channel_table()
elif show_table=="Playlists":
    show_playlists_table()
elif show_table=="Videos":
    show_videos_table()
elif show_table=="Comments":
    show_comments_table()

#SQL Connection
mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Postsql1",
                        database="Youtube_Data",
                        port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your Question",("1.Retrieve all Channels and Video Names",
                                            "2.Channels with most number of videos",
                                            "3.Top 10 most viewed videos",
                                            "4.Comments in each video",
                                            "5.Most Liked Videos",
                                            "6.Likes of all videos",
                                            "7.Views of each Channel",
                                            "8.Videos published in 2022",
                                            "9.Average Duration of all Videos in each Channel",
                                            "10.Videos with highest number of Comments"))

if question=="1.Retrieve all Channels and Video Names":
    q1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(q1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["Video Title","Channel Name"])
    st.write(df)

elif question=="2.Channels with most number of videos":
    q2='''select channel_name as Channelname,total_videos as Noofvideos from channels
            order by total_videos desc'''
    cursor.execute(q2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=["Channel Name","No.of Videos"])
    st.write(df1)

elif question=="3.Top 10 most viewed videos":
        q3='''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
        cursor.execute(q3)
        mydb.commit()
        t3=cursor.fetchall()
        df2=pd.DataFrame(t3,columns=["Views","Channel Name","Video Title"])
        st.write(df2)

elif question=="4.Comments in each video":
        q4='''select comments as no_comments, title as video_title from videos
                where comments is not null '''
        cursor.execute(q4)
        mydb.commit()
        t4=cursor.fetchall()
        df3=pd.DataFrame(t4,columns=["No.of Comments","Video Title"])
        st.write(df3)

elif question=="5.Most Liked Videos":
        q5='''select  title as video_title, channel_name as channelname, likes as likecount from videos
                where likes is not null order by likes desc'''
        cursor.execute(q5)
        mydb.commit()
        t5=cursor.fetchall()
        df4=pd.DataFrame(t5,columns=["Video Title","Channel Name","Like Count"])
        st.write(df4)

elif question=="6.Likes of all videos":
        q6='''select  title as video_title, channel_name as channelname, likes as likecount from videos
                where likes is not null order by likes desc'''
        cursor.execute(q6)
        mydb.commit()
        t6=cursor.fetchall()
        df5=pd.DataFrame(t6,columns=["Video Title","Channel Name","Like Count"])
        st.write(df5)
elif question=="7.Views of each Channel":
    q7='''select channel_name as channelname, views as totalviews from channels'''
    cursor.execute(q7)
    mydb.commit()
    t7=cursor.fetchall()
    df6=pd.DataFrame(t7,columns=["Channel Name","Total Views"])
    st.write(df6)

elif question=="8.Videos published in 2022":
    q8='''select channel_name as channelname, title as videotitle, published_date as videorelease from videos
            where extract(year from published_date)=2022'''
    cursor.execute(q8)
    mydb.commit()
    t8=cursor.fetchall()
    df7=pd.DataFrame(t8,columns=["Channel Name","Title","Published Date"])
    st.write(df7)

elif question=="9.Average Duration of all Videos in each Channel":
     q9='''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
     cursor.execute(q9)
     mydb.commit()
     t9=cursor.fetchall()
     df8=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])
     T9=[]
     for index,row in df8.iterrows():
        channel_title=row["Channel Name"]
        average_duration=row["Average Duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
     df9=pd.DataFrame(T9)
     st.write(df9)

elif question=="10.Videos with highest number of Comments":
    q10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(q10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Video Title","Channel Name","Comments Count"])
    st.write(df10)





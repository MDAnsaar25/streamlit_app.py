import pandas as pd
import streamlit as st
import datetime
from googleapiclient.discovery import build
from sqlalchemy import create_engine, Column, Integer, String, Text, TIMESTAMP, BigInteger, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from urllib.parse import quote_plus
import matplotlib.pyplot as plt
import seaborn as sns

# Define SQLAlchemy base
Base = declarative_base()

# Define ChannelDetails class for channel_details table
class ChannelDetails(Base):
    __tablename__ = 'channel_details'

    Channel_Id = Column(String(80), primary_key=True)
    Channel_Name = Column(String(100))
    Subscribers = Column(BigInteger)
    Views = Column(BigInteger)
    Total_Videos = Column(Integer)
    Channel_Description = Column(Text)
    Playlist_Id = Column(String(80))

# Define PlaylistInformation class for playlist_information table
class PlaylistInformation(Base):
    __tablename__ = 'playlist_information'

    Playlist_Id = Column(String(100), primary_key=True)
    Title = Column(String(100))
    Channel_Id = Column(String(100))
    Channel_Name = Column(String(100))
    PublishedAt = Column(TIMESTAMP)
    Video_Count = Column(Integer)

# Define VideoInformation class for videoinformation table
class VideoInformation(Base):
    __tablename__ = 'videoinformation'

    Video_Id = Column(String(30), primary_key=True)
    Channel_Name = Column(String(100))
    Channel_Id = Column(String(100))
    Title = Column(String(150))
    Tags = Column(Text)
    Thumbnail = Column(String(200))
    Description = Column(Text)
    Published_Date = Column(TIMESTAMP)
    Duration = Column(String(50))
    Views = Column(BigInteger)
    Likes = Column(BigInteger)
    Comments = Column(Integer)
    Favorite_Count = Column(Integer)
    Definition = Column(String(10))
    Caption_Status = Column(String(50))

    # Relationship with comments
    comments = relationship("CommentInformation", back_populates="video")

# Define CommentInformation class for comments table
class CommentInformation(Base):
    __tablename__ = 'comment_information'

    Comment_Id = Column(Integer, primary_key=True, autoincrement=True)
    Video_Id = Column(String(30), ForeignKey('videoinformation.Video_Id'))
    Comment_Text = Column(Text)
    Comment_Author = Column(String(100))
    Comment_Published_Date = Column(TIMESTAMP)

    video = relationship("VideoInformation", back_populates="comments")

# Encoding the password
password = quote_plus('Mysql@1997!')

# Create engine and session (connecting with Mysql)
engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
Session = sessionmaker(bind=engine)

# Define the function to create tables
def create_tables():
    Base.metadata.create_all(engine)

create_tables()
# Function to collect data from the YouTube API and store it in the database
def collect_and_store_data(channel_id):
    # Check if the channel already exists in the database
    session = Session()
    existing_channel = session.query(ChannelDetails).filter_by(Channel_Id=channel_id).first()
    session.close()

    if existing_channel:
        return "Channel already exists in the database."

    # Establish a connection to the YouTube API
    youtube = build("youtube", "v3", developerKey="AIzaSyDg66p4ge8m4FVMSwnDGWbjkcZTYtbX7dg")  # Replace YOUR_API_KEY with your actual API key
    
    # Retrieve channel information
    channel_info_request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )
    channel_info_response = channel_info_request.execute()

    channel_data = channel_info_response["items"][0]
    channel_snippet = channel_data["snippet"]
    channel_statistics = channel_data["statistics"]
    channel_content_details = channel_data.get("contentDetails", {})

    # Extract relevant data from the response
    channel_name = channel_snippet["title"]
    subscribers = int(channel_statistics["subscriberCount"])
    views = int(channel_statistics["viewCount"])
    total_videos = int(channel_statistics["videoCount"])
    channel_description = channel_snippet.get("description", "")
    playlist_id = channel_content_details.get("relatedPlaylists", {}).get("uploads", "")

    # Store channel information in the database
    session = Session()
    new_channel = ChannelDetails(Channel_Id=channel_id, Channel_Name=channel_name, Subscribers=subscribers,
                                 Views=views, Total_Videos=total_videos, Channel_Description=channel_description,
                                 Playlist_Id=playlist_id)
    session.add(new_channel)
    session.commit()
    session.close()

    # Retrieve playlist information if playlist_id exists
    if playlist_id:
        playlist_info_request = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50
        )
        playlist_info_response = playlist_info_request.execute()

        for playlist_item in playlist_info_response["items"]:
            playlist_id = playlist_item["id"]
            title = playlist_item["snippet"]["title"]
            published_at = datetime.datetime.strptime(playlist_item["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
            
            # retrieve video count from content details, if available
            content_details = playlist_item["snippet"].get("contentDetails", {})
            video_count = content_details.get("itemCount", 0)

            # Store playlist information in the database
            session = Session()
            new_playlist = PlaylistInformation(Playlist_Id=playlist_id, Title=title, Channel_Id=channel_id,
                                               Channel_Name=channel_name, PublishedAt=published_at, Video_Count=video_count)
            session.add(new_playlist)
            session.commit()
            session.close()

    # Retrieve videos from the channel's playlist
    video_ids = []
    next_page_token = None

    while True:
        playlist_items_request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        playlist_items_response = playlist_items_request.execute()

        for item in playlist_items_response["items"]:
            video_ids.append(item["snippet"]["resourceId"]["videoId"])

        next_page_token = playlist_items_response.get("nextPageToken")

        if not next_page_token:
            break

    # Retrieve video details and store them in the database
    for video_id in video_ids:
        video_info_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        video_info_response = video_info_request.execute()

        video_data = video_info_response["items"][0]
        video_snippet = video_data["snippet"]
        video_content_details = video_data["contentDetails"]
        video_statistics = video_data["statistics"]

        # Extract relevant data from the response
        title = video_snippet["title"]
        tags = video_snippet.get("tags", [])
        thumbnail = video_snippet["thumbnails"]["default"]["url"]
        description = video_snippet.get("description", "")
        published_at = datetime.datetime.strptime(video_snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
        duration = video_content_details["duration"]
        views = int(video_statistics.get("viewCount", 0))
        likes = int(video_statistics.get("likeCount", 0))
        comments = int(video_statistics.get("commentCount", 0))
        favorite_count = int(video_statistics.get("favoriteCount", 0))
        definition = video_content_details["definition"]
        caption_status = video_content_details["caption"]

                # Store video information in the database
        session = Session()
        new_video = VideoInformation(Video_Id=video_id, Channel_Name=channel_name, Channel_Id=channel_id, Title=title,
                                     Tags=",".join(tags), Thumbnail=thumbnail, Description=description,
                                     Published_Date=published_at, Duration=duration, Views=views, Likes=likes,
                                     Comments=comments, Favorite_Count=favorite_count, Definition=definition,
                                     Caption_Status=caption_status)
        session.add(new_video)
        session.commit()
        session.close()

        # Retrieve comments for the current video
        comment_threads_request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100
        )
        comment_threads_response = comment_threads_request.execute()

        for item in comment_threads_response["items"]:
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comment_text = comment["textDisplay"]
            comment_author = comment["authorDisplayName"]
            comment_published_at = datetime.datetime.strptime(comment["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")

            # Store comment information in the database
            session = Session()
            new_comment = CommentInformation(Video_Id=video_id, Comment_Text=comment_text,
                                             Comment_Author=comment_author, Comment_Published_Date=comment_published_at)
            session.add(new_comment)
            session.commit()
            session.close()

# Rest of the code remains the same...
#import streamlit as st

# Streamlit interface for user input and buttons
st.title("YouTube Data Harvesting and Ware Housing")

# Input field for entering channel ID
channel_id = st.text_input("Enter YouTube Channel ID:")

# Button to collect and store data for the given channel ID
if st.button("Collect and Store Data"):
    if channel_id:
        # Call function to collect and store data for the given channel ID
        st.write(f"Collecting and storing data for Channel ID: {channel_id}...")

        # Call the function to collect and store data for the given channel ID
        collect_and_store_data(channel_id)
        st.success("Data collected and stored successfully!")
    else:
        st.warning("Please enter a YouTube Channel ID.")
# Button to show channel details
if st.button("Show Channel Details"):
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    
    # Fetch all channel details from the database
    df_channel_details = pd.read_sql("SELECT * FROM channel_details", engine)
    
    # Display channel details
    if not df_channel_details.empty:
        st.write("## Channel Details")
        st.write(df_channel_details)
        
        # Iterate over each channel to calculate and display total number of videos and likes
        for index, row in df_channel_details.iterrows():
            channel_id = row['Channel_Id']
            total_videos = row['Total_Videos']
            
            # Check if 'Likes' column exists in the dataframe
            if 'Likes' in df_channel_details.columns:
                total_likes = row['Likes']
                st.write(f"Total Number of Videos for Channel ID {channel_id}: {total_videos}")
                st.write(f"Total Likes for Channel ID {channel_id}: {total_likes}")
            #else:
                #st.warning(f"Likes information not available for Channel ID {channel_id}.")
    else:
        st.warning("No channel details found in the database.")


# Define Streamlit interface continued...
st.write('<h2 style="color:red;">Data Analysis</h2>', unsafe_allow_html=True)
st.write("Choose a question from the dropdown menu to analyze the data:")

# Define questions and corresponding SQL queries
questions = [
    "All the videos and the channel name",
    "Top 10 most viewed videos",
    "Comments in each video",
    "Top Videos with the highest likes",
    "Overall Likes of all videos",
    "Overall Views of each channel",
    "Videos published in the year 2022",
    #"Overall Average duration of all videos in each channel",
    "Videos with the highest number of comments"
]

queries = [
    '''SELECT Title AS videos, Channel_Name AS channelname FROM videoinformation''',
    '''SELECT Views AS views, Channel_Name AS channelname, Title AS videotitle FROM videoinformation WHERE Views IS NOT NULL ORDER BY Views DESC LIMIT 10''',
    '''SELECT Comments AS no_comments, Title AS videotitle FROM videoinformation WHERE Comments IS NOT NULL''',
    '''SELECT Title AS videotitle, Channel_Name AS channelname, Likes AS likecount FROM videoinformation WHERE Likes IS NOT NULL ORDER BY Likes DESC''',
    '''SELECT Likes AS likecount, Title AS videotitle FROM videoinformation''',
    '''SELECT Channel_Name AS channelname, Views AS totalviews FROM videoinformation''',
    '''SELECT Title AS video_title, Published_Date AS videorelease, Channel_Name AS channelname FROM videoinformation WHERE EXTRACT(YEAR FROM Published_Date) = 2022''',
    #'''SELECT Channel_Name AS channelname, AVG(Duration) AS avg_duration 
   #FROM videoinformation 
   #WHERE Duration != 'PT0S' AND Duration != '' AND Duration IS NOT NULL
   #GROUP BY Channel_Name''',
    '''SELECT Title AS videotitle, Channel_Name AS channelname, Comments AS comments FROM videoinformation WHERE Comments IS NOT NULL ORDER BY Comments DESC'''
]


# Streamlit UI for selecting questions
question = st.selectbox("Select your question", questions)


# Execute selected query and display results
if question:
    query_index = questions.index(question)
    query = queries[query_index]
    print("Executing query:", query)  # Add this line for debugging
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    df = pd.read_sql(query, engine)
    print("Query result:")
    print(df)  # Add this line for debugging
    st.write(df)


# Data Analysis Part

# Function to analyze average duration of all videos in each channel
def analyze_average_duration():
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    query = """
        SELECT Channel_Name AS channelname, AVG(duration_seconds) AS avg_duration_seconds
        FROM (
            SELECT Channel_Name, TIME_TO_SEC(SUBSTRING(Duration, 3)) AS duration_seconds
            FROM videoinformation
            WHERE Duration NOT LIKE 'PT0S' AND Duration != ''
        ) AS durations
        GROUP BY Channel_Name
    """
    df = pd.read_sql(query, engine)
    return df
# Function to fetch playlist details from the database
def fetch_playlist_details():
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    df_playlist_details = pd.read_sql("SELECT * FROM playlist_information", engine)
    return df_playlist_details

# Button to show playlist details
if st.button("Show Playlist Details"):
    df_playlist = fetch_playlist_details()
    if not df_playlist.empty:
        st.write("## Playlist Details")
        st.write(df_playlist)
    else:
        st.warning("No playlist details found in the database.")
        
# Function to fetch video details from the database
def fetch_video_details():
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    df_video_details = pd.read_sql("SELECT * FROM videoinformation", engine)
    return df_video_details

# Button to show video details
if st.button("Show Video Details"):
    df_video = fetch_video_details()
    if not df_video.empty:
        st.write("## Video Details")
        st.write(df_video)
    else:
        st.warning("No video details found in the database.")

# Display average duration by channel
if st.button("Show Average Duration by Channel"):
    df_avg_duration = analyze_average_duration()
    st.write("## Average Duration by Channel")
    st.write(df_avg_duration)

# Function to analyze total views by channel
def analyze_views_by_channel():
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    df = pd.read_sql("SELECT Channel_Name, SUM(Views) AS Total_Views FROM videoinformation GROUP BY Channel_Name", engine)
    return df

# Function to analyze total likes by channel
def analyze_likes_by_channel():
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    df = pd.read_sql("SELECT Channel_Name, SUM(Likes) AS Total_Likes FROM videoinformation GROUP BY Channel_Name", engine)
    return df

# Function to analyze total comments by channel
def analyze_comments_by_channel():
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    df = pd.read_sql("SELECT Channel_Name, SUM(Comments) AS Total_Comments FROM videoinformation GROUP BY Channel_Name", engine)
    return df

# Function to analyze total videos by channel
def analyze_videos_by_channel():
    engine = create_engine(f'mysql+mysqlconnector://root:{password}@127.0.0.1/youtube_ready')
    df = pd.read_sql("SELECT Channel_Name, COUNT(*) AS Total_Videos FROM videoinformation GROUP BY Channel_Name", engine)
    return df





# Define the visualization option based on user input
visualization_option = st.selectbox("Select a visualization option:", ("Total Videos by Channel", "Total Likes by Channel", "Total Comments by Channel"))

# Define functions for data visualization
def visualize_total_videos_pie():
    df = analyze_videos_by_channel()
    plt.figure(figsize=(10, 10))
    patches, texts, autotexts = plt.pie(df['Total_Videos'], labels=df['Channel_Name'], autopct='%1.1f%%', startangle=140)
    
    # Adjust label positions
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
    
    plt.title("Total Videos by Channel")
    return plt.gcf()

def visualize_total_likes_pie():
    df = analyze_likes_by_channel()
    plt.figure(figsize=(10, 10))
    patches, texts, autotexts = plt.pie(df['Total_Likes'], labels=df['Channel_Name'], autopct='%1.1f%%', startangle=140)
    
    # Adjust label positions
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
    
    plt.title("Total Likes by Channel")
    return plt.gcf()

def visualize_total_comments_pie():
    df = analyze_comments_by_channel()
    plt.figure(figsize=(10, 10))
    patches, texts, autotexts = plt.pie(df['Total_Comments'], labels=df['Channel_Name'], autopct='%1.1f%%', startangle=140)
    
    # Adjust label positions
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
    
    plt.title("Total Comments by Channel")
    return plt.gcf()

# Display pie chart based on selected option
if visualization_option == "Total Videos by Channel":
    fig = visualize_total_videos_pie()
    st.pyplot(fig)
elif visualization_option == "Total Likes by Channel":
    fig = visualize_total_likes_pie()
    st.pyplot(fig)
elif visualization_option == "Total Comments by Channel":
    fig = visualize_total_comments_pie()
    st.pyplot(fig)

# Data visualization options
visualization_option = st.selectbox("Select a visualization option:", ("Total Views by Channel", "Total Likes by Channel", "Total Comments by Channel"))

# Define functions for data visualization
def visualize_total_views():
    df = analyze_views_by_channel()
    plt.figure(figsize=(10, 6))
    sns.barplot(x="Total_Views", y="Channel_Name", data=df)
    plt.xlabel("Total Views")
    plt.ylabel("Channel Name")
    plt.title("Total Views by Channel")
    return plt.gcf()

def visualize_total_likes():
    df = analyze_likes_by_channel()
    plt.figure(figsize=(10, 6))
    sns.barplot(x="Total_Likes", y="Channel_Name", data=df)
    plt.xlabel("Total Likes")
    plt.ylabel("Channel Name")
    plt.title("Total Likes by Channel")
    return plt.gcf()

def visualize_total_comments():
    df = analyze_comments_by_channel()
    plt.figure(figsize=(10, 6))
    sns.barplot(x="Total_Comments", y="Channel_Name", data=df)
    plt.xlabel("Total Comments")
    plt.ylabel("Channel Name")
    plt.title("Total Comments by Channel")
    return plt.gcf()

# Display visualization based on selected option
if visualization_option == "Total Views by Channel":
    fig = visualize_total_views()
    st.pyplot(fig)
elif visualization_option == "Total Likes by Channel":
    fig = visualize_total_likes()
    st.pyplot(fig)
elif visualization_option == "Total Comments by Channel":
    fig = visualize_total_comments()
    st.pyplot(fig)

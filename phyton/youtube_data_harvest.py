import pymysql as py
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime

# Function to create or alter the youtube_videos table to include likes, dislikes, and comments columns
def create_youtube_videos_table():
    try:
        # Connect to the MySQL database
        connection = py.connect(
            host="localhost", 
            user="root", 
            password="Ashrar@23", 
            database="capstone"
        )
        cursor = connection.cursor()

        # Create the youtube_videos table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS youtube_videos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            description TEXT,
            published_at DATETIME,
            video_url VARCHAR(255),
            likes INT DEFAULT 0,
            dislikes INT DEFAULT 0,
            comments INT DEFAULT 0
        );
        """
        cursor.execute(create_table_query)

        # Check the existing columns in the youtube_videos table
        cursor.execute("DESCRIBE youtube_videos;")
        columns = [column[0] for column in cursor.fetchall()]

        # Add missing columns individually
        if "likes" not in columns:
            cursor.execute("ALTER TABLE youtube_videos ADD COLUMN likes INT DEFAULT 0;")
        if "dislikes" not in columns:
            cursor.execute("ALTER TABLE youtube_videos ADD COLUMN dislikes INT DEFAULT 0;")
        if "comments" not in columns:
            cursor.execute("ALTER TABLE youtube_videos ADD COLUMN comments INT DEFAULT 0;")
        
        # Commit the changes and close the connection
        connection.commit()
        connection.close()

        st.success("Table created/altered successfully.")

    except py.MySQLError as e:
        st.error(f"Error creating table: {e}")

# Function to convert YouTube API datetime format to MySQL-compatible format
def convert_to_mysql_datetime(youtube_datetime):
    if youtube_datetime.endswith('Z'):
        youtube_datetime = youtube_datetime[:-1]
    youtube_datetime = youtube_datetime.replace('T', ' ')
    
    try:
        return datetime.strptime(youtube_datetime, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        st.error(f"Error converting datetime: {e}")
        return None

# Function to retrieve channel details (name, subscribers, video count)
def get_channel_details(youtube, channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = request.execute()
        if response['items']:
            channel = response['items'][0]
            return {
                "channel_name": channel["snippet"]["title"],
                "subscribers": channel["statistics"]["subscriberCount"],
                "total_videos": channel["statistics"]["videoCount"],
            }
        else:
            st.warning("Channel not found.")
            return None
    except Exception as e:
        st.error(f"Error fetching channel details: {e}")
        return None

# Function to retrieve playlist IDs from the channel
def get_playlists(youtube, channel_id):
    playlists = []
    try:
        request = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50
        )
        response = request.execute()

        # Check if 'items' exist in the response
        if 'items' in response:
            for item in response["items"]:
                playlists.append({
                    "playlist_id": item["id"],
                    "playlist_name": item["snippet"]["title"]
                })
        return playlists
    except Exception as e:
        st.error(f"Error fetching playlists: {e}")
        return []

# Function to retrieve video details (likes, dislikes, comments) from a playlist
def get_video_details(youtube, playlist_id):
    videos = []
    try:
        request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50
        )
        response = request.execute()

        # Check if 'items' exist in the response
        if 'items' in response:
            for item in response["items"]:
                video_id = item["snippet"]["resourceId"]["videoId"]
                video_info = get_video_info(youtube, video_id)
                videos.append(video_info)
        return videos
    except Exception as e:
        st.error(f"Error fetching video details: {e}")
        return []

# Function to get detailed information about a video (likes, dislikes, comments)
def get_video_info(youtube, video_id):
    try:
        request = youtube.videos().list(
            part="statistics,snippet",
            id=video_id
        )
        response = request.execute()
        if 'items' in response and len(response['items']) > 0:
            video = response["items"][0]
            video_data = {
                "title": video["snippet"]["title"],
                "description": video["snippet"]["description"],
                "published_at": convert_to_mysql_datetime(video["snippet"]["publishedAt"]),
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "likes": video["statistics"].get("likeCount", 0),
                "dislikes": video["statistics"].get("dislikeCount", 0),
                "comments": video["statistics"].get("commentCount", 0)
            }
            return video_data
        else:
            st.warning(f"Video with ID {video_id} not found.")
            return None
    except Exception as e:
        st.error(f"Error fetching video info: {e}")
        return None

# Function to store YouTube video data in MySQL database
def store_youtube_data_in_db(video_data):
    try:
        connection = py.connect(
            host="localhost", 
            user="root", 
            password="Ashrar@23", 
            database="capstone"
        )
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO youtube_videos (title, description, published_at, video_url, likes, dislikes, comments)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        for video in video_data:
            cursor.execute(insert_query, (
                video['title'], 
                video['description'], 
                video['published_at'], 
                video['video_url'],
                video['likes'],
                video['dislikes'],
                video['comments']
            ))

        connection.commit()
        connection.close()
        st.success("YouTube video data has been stored successfully in the database.")

    except py.MySQLError as e:
        st.error(f"Error storing data in the database: {e}")

# Streamlit app to display data
def main():
    st.title("YouTube Channel and Video Data Display")

    # API Key for YouTube API
    api_key = "YOUR_YOUTUBE_API_KEY"  # Replace with your actual YouTube API key
    youtube = build("youtube", "v3", developerKey=api_key)

    # Input for YouTube Channel ID
    channel_id = st.text_input("Enter YouTube Channel ID:")

    if channel_id:
        # Retrieve channel details
        channel_details = get_channel_details(youtube, channel_id)
        if channel_details:
            st.write(f"**Channel Name**: {channel_details['channel_name']}")
            st.write(f"**Subscribers**: {channel_details['subscribers']}")
            st.write(f"**Total Videos**: {channel_details['total_videos']}")

            # Retrieve and display playlists
            st.subheader("Playlists")
            playlists = get_playlists(youtube, channel_id)
            if playlists:
                for playlist in playlists:
                    st.write(f"**Playlist Name**: {playlist['playlist_name']}")
                    st.write(f"**Playlist ID**: {playlist['playlist_id']}")

                    # Retrieve video details for each playlist
                    st.subheader(f"Videos in Playlist: {playlist['playlist_name']}")
                    video_data = get_video_details(youtube, playlist['playlist_id'])

                    if video_data:
                        store_youtube_data_in_db(video_data)
                        # Displaying the video data in a nice table
                        video_df = pd.DataFrame(video_data)
                        st.dataframe(video_df)

            else:
                st.warning("No playlists found for this channel.")
        else:
            st.warning("Channel not found.")

if __name__ == "__main__":
    create_youtube_videos_table()  # Ensure table exists before starting
    main()

from dotenv import load_dotenv
import os
import base64
from requests import post
import json
import requests
from datetime import datetime
import sys
import time
import psycopg2

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
HOST = os.getenv("DB_HOST")
DATABASE = os.getenv("DB_DATABASE")
SCHEMA = os.getenv("DB_SCHEMA")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
PORT = os.getenv("DB_PORT")
TABLE = os.environ.get("DB_TABLE")
formatted_date = datetime.now().strftime("%Y.%m.%d %H:%M:%S")

def get_token():
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

def get_auth(token):
    return {"Authorization": f"Bearer {token}"}

def get_playlist(country, playlist_id):
    token = get_token()
    headers = get_auth(token)
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}?limit=200"
    response = requests.get(url, headers=headers)
    playlist = response.json()
    return playlist

def get_markets(country_code):
    token = get_token()
    headers = get_auth(token)
    url = "https://api.spotify.com/v1/search?query=foo&type=track&market=TR&offset=0&limit=1"
    response = requests.get(url, headers=headers)
    
    trends = response.json()
    print(trends)
    return trends

def get_artist(artist_id):
    time.sleep(0.1)
    token = get_token()
    headers = get_auth(token)
    href = f"https://api.spotify.com/v1/artists/{artist_id}"
    response = requests.get(href, headers=headers)
    artist_info = response.json()
    genre = artist_info["genres"]
    return genre

def get_audio_analysis(track_id):
    token = get_token()
    headers = get_auth(token)
    url = f"https://api.spotify.com/v1/audio-features/{track_id}"
    response = requests.get(url, headers=headers)
    audio_analysis  =      response.json()
    danceability    =      audio_analysis["danceability"]
    energy          =      audio_analysis["energy"]
    key             =      audio_analysis["key"]
    loudness        =      audio_analysis["loudness"]
    mode            =      audio_analysis["mode"]
    speechiness     =      audio_analysis["speechiness"]
    acousticness    =      audio_analysis["acousticness"]
    instrumentalness=      audio_analysis["instrumentalness"]
    liveness        =      audio_analysis["liveness"]
    valence         =      audio_analysis["valence"]
    tempo           =      audio_analysis["tempo"]

    audio_features = {"danceability":danceability,
                    "energy":energy, 
                    "key":key, 
                    "loudness":loudness,
                    "mode":mode,
                    "speechiness":speechiness,
                    "acousticness":acousticness,
                    "instrumentalness":instrumentalness,
                    "liveness":liveness,
                    "valence":valence,
                    "tempo":tempo}
    return audio_features

def parse(playlist, country):
    song_list = []
    # Extract the track information from the playlist data  
    tracks = playlist["tracks"]["items"]
    track_rank = 1
    for track in tracks:
        duration_ms = track["track"]["duration_ms"]
        added_at = track["added_at"]
        popularity = track["track"]["popularity"] 
        track_name = track["track"]["name"]
        artists = ", ".join([artist["name"] for artist in track["track"]["artists"]])
        album_names = track["track"]["album"]["name"]
        release_date = track["track"]["album"]["release_date"]
        artist_id = track["track"]["artists"][0]["id"]
        track_id = track["track"]["id"]
        #genre = get_artist(artist_id)["genres"]

        try:
            genre = ", ".join(get_artist(artist_id))
        except Exception as e:
            genre = None
            print(f"An error occurred while getting genres for artist ID {artist_id}: {e}")
        
        track_feauters = get_audio_analysis(track_id)
        danceability = track_feauters["danceability"]
        energy = track_feauters["energy"]
        key = track_feauters["key"]
        loudness = track_feauters["loudness"]
        mode = track_feauters["mode"]
        speechiness = track_feauters["speechiness"]
        acousticness = track_feauters["acousticness"]
        instrumentalness = track_feauters["instrumentalness"]
        liveness = track_feauters["liveness"]
        valence = track_feauters["valence"]
        tempo = track_feauters["tempo"]
        
        print("track_rank is : ",track_rank)
        print("playlist_country is : ",country)
        print("duration_ms is : ",duration_ms)
        print("added_at is : ",added_at)
        print("popularity is : ",popularity)
        print("track_name is : ",track_name)
        print("album_names is : ",album_names)
        print("release_date is : ",release_date)
        print("scrape_date is : ",formatted_date)
        print("artists is : ",artists)
        print("artist_id is : ",artist_id)
        print("track_id is : ",track_id)
        print("genre is : ",genre)
        print("track_feauters is : ",track_feauters.items())
        print("-------------------------------------------------")

        song_list.append([track_rank, country, added_at, track_name, track_id, popularity, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, artists, artist_id, album_names, genre, release_date, formatted_date])
        track_rank += 1

    return song_list

def db_insert(song_list):

    conn = None
    # Connect to the database
    print("before connection")
    conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
    print("after connection")
    print("connection is created")

    # Create a cursor object
    cur = conn.cursor()
    print("cursor is created")

    # Create schema if not exists
    cur.execute(f'''CREATE SCHEMA IF NOT EXISTS {SCHEMA}''')

    # Create table if not exists
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (id serial4 NOT NULL, track_rank integer, country text, added_at date, track_name text, track_id text, popularity integer, danceability float, energy float, key float, loudness float, mode float, speechiness float, acousticness float, instrumentalness float, liveness float, valence float, tempo float,  artists text, artist_id text, album_names text, genre text,  release_date date, scrape_date timestamp, CONSTRAINT {TABLE}_pkey PRIMARY KEY (id))""")
    query = f"""INSERT INTO {SCHEMA}.{TABLE} (track_rank, country, added_at, track_name, track_id, popularity,  danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo,artists, artist_id, album_names, genre,  release_date, scrape_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    print("products are inserted to specified db on postgre sql")
    # Use a loop to insert the data into the database
    for raw in song_list:
        values = (raw[0], raw[1], raw[2], raw[3],raw[4], raw[5], raw[6], raw[7],raw[8], raw[9], raw[10], raw[11],raw[12], raw[13], raw[14], raw[15],raw[16], raw[17], raw[18], raw[19], raw[20], raw[21], raw[22])
        print("values are: ",values)
        cur.execute(query, values)  
    print(f"Inserted row with ID")

    """
    values = [(i,) for i in self.product_list]
    print(values)
    cur.executemany(query, values)
    """
    
    # Commit the changes to the database
    conn.commit()
    print("changes are commited")

    # Close the cursor and connection
    cur.close()
    conn.close()
    print("cursor and connection are closed")

"""
def main():    
    with open("regions.json", "r") as file:
        arguments = json.load(file)["playlist_ids"]

    for argument in arguments:
        print("Country parse for the country:", argument)
        country_code = argument[0]
        country_id = argument[1]
        print("id is ",country_id, "for the country:",country_code)
        playlist = get_playlist(country_code, country_id) 
        print("playlist is ready")
        print("playlist parse starts")
        song_list = parse(playlist, country_code)
        print("df is ready")
        db_insert(song_list)
        
    return "Success!"

if __name__=='__main__':
    main()
"""
def lambda_handler(event, context):
    given_argument = event['country']
    print(f"Scraping request for the {given_argument} is received")

    with open("regions.json", "r") as file:
        arguments = json.load(file)["playlist_ids"]

    for argument in arguments:
        print("Country parse for the country:", argument)
        country_code = argument[0]
        country_id = argument[1]
        print("id is ",country_id, "for the country:",country_code)
        playlist = get_playlist(country_code, country_id) 
        print("playlist is ready")
        print("playlist parse starts")
        df = parse(playlist, country_code)
        print("df is ready")
        db_insert(df)
   
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

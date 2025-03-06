import azure.functions as func
import logging
import os
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone
from io import StringIO


app = func.FunctionApp()

def album(data):
    album_id_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_elements = {'album_id':album_id,'album_name':album_name,'album_release_date':album_release_date,'album_total_tracks':album_total_tracks,'album_url':album_url}
        album_id_list.append(album_elements)
    return album_id_list  

def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {
            'song_id' : song_id,
            'song_name': song_name,
            'song_duration' : song_duration,
            'song_url': song_url,
            'song_popularity' : song_popularity,
            'song_added': song_added,
            'album_id': album_id,
            'artist_id': artist_id
                        
        }
        song_list.append(song_element)
    return song_list    


def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id': artist['id'],'artist_name': artist['name'],'external_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list  

AZURE_STORAGE_CONNECTION_STRING = "DefaultEnhaBJ2+ASthiYoDA==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "spotifycontainer"
FOLDER_PATH = "raw_data/processed/"

@app.blob_trigger(arg_name="myblob", path="spotifycontainer",
                               connection="DefaultEndpointsProtocol=36LFfOhRy6YI/gMxz1Q+DYfgwwoD6YhaBJ2+ASthiYoDA==;EndpointSuffix=core.windows.net") 
def blob_trigger_extract(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")


       

      # Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # List blobs in the specified folder
spotify_data = []
spotify_keys = []
blobs = container_client.list_blobs(name_starts_with=FOLDER_PATH)
for blob in blobs:
            blob_name = blob.name
            print(f"📂 Fetching file: {blob.name}") 
            if blob_name.split('.')[-1] == "json":
                # Download the blob content
                blob_client = container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob().readall()
                json_content = json.loads(blob_data)
                spotify_data.append(json_content)
                spotify_keys.append(blob_name)

for data in spotify_data:
            album_list = album(data)
            artist_list = artist(data)
            song_list = songs(data)

            album_df = pd.DataFrame.from_dict(album_list)    
            album_df = album_df.drop_duplicates(subset=['album_id'])
                    
            artist_df = pd.DataFrame.from_dict(artist_list)    
            artist_df = artist_df.drop_duplicates(subset=['artist_id'])     
                
            song_df = pd.DataFrame.from_dict(song_list)
            album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'],errors = "coerce")


            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            blob_path_song = f"transformed_data/song_data/songs_tranformed_{timestamp}.csv" 
            blob_client_song = container_client.get_blob_client(blob_path_song)
            song_buffer = StringIO()
            song_df.to_csv(song_buffer,index=False)
            song_content = song_buffer.getvalue()
            blob_client_song.upload_blob(song_content, overwrite=True)

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            blob_path_album = f"transformed_data/album_data/songs_tranformed_{timestamp}.csv" 
            blob_client_album = container_client.get_blob_client(blob_path_album)
            album_buffer = StringIO()
            album_df.to_csv(album_buffer,index=False)
            album_content = album_buffer.getvalue()
            blob_client_album.upload_blob(album_content, overwrite=True)

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            blob_path_artist = f"transformed_data/artist_data/songs_tranformed_{timestamp}.csv" 
            blob_client_artist = container_client.get_blob_client(blob_path_artist)
            artist_buffer = StringIO()
            artist_df.to_csv(artist_buffer,index=False)
            artist_content = artist_buffer.getvalue()
            blob_client_artist.upload_blob(artist_content, overwrite=True)

# Define the source and destination prefixes
source_prefix = "raw_data/processed/"
destination_prefix = "raw_data/to_processed/"

# Create the BlobServiceClient and ContainerClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# List all blobs under the source prefix and copy each to the destination prefix
for blob in container_client.list_blobs(name_starts_with=source_prefix):
    source_blob_name = blob.name
    # Remove the source prefix and append the remainder to the destination prefix
    relative_path = source_blob_name[len(source_prefix):]
    destination_blob_name = os.path.join(destination_prefix, relative_path).replace("\\", "/")
    
    source_blob_client = container_client.get_blob_client(source_blob_name)
    destination_blob_client = container_client.get_blob_client(destination_blob_name)
    
    # Start the copy operation using the source blob's URL
    copy_props = destination_blob_client.start_copy_from_url(source_blob_client.url)
    print(f"Copying '{source_blob_name}' to '{destination_blob_name}'. Copy status: {copy_props['copy_status']}")            
    

    copy_status = copy_props["copy_status"]
    while copy_status == "pending":
        props = destination_blob_client.get_blob_properties()
        copy_status = props.copy.status

    if copy_status.lower() == "success":
        print(f"Copied '{source_blob_name}' successfully.")
        # Delete the source blob after successful copy
        source_blob_client.delete_blob()
        print(f"Deleted source blob '{source_blob_name}'.")
    else:
        print(f"Failed to copy '{source_blob_name}'. Copy status: {copy_status}")
  
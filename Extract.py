import logging
import os
import json
from datetime import datetime, timezone
import logging
import os
import azure.functions as func
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 9 * * 1", arg_name="myTimer", run_on_startup=True,
                   use_monitor=False)
def testingfunc(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

SPOTIPY_CLIENT_ID = "726042aadd7"
SPOTIPY_CLIENT_SECRET = "2420bae"

if not SPOTIPY_CLIENT_ID or not SPOTIPY_CLIENT_SECRET:
    raise ValueError("Missing SPOTIPY_CLIENT_ID or SPOTIPY_CLIENT_SECRET")

print("started") 
client_credentials_manager = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID,client_secret=SPOTIPY_CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
playlist_link = "https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE"
playlist_URI = playlist_link.split("/")[-1]
data = sp.playlist_tracks(playlist_URI)


 # Connect to Azure Blob Storage
connect_str = "DefaultEndpointOhre.windows.net"
if not connect_str:
    raise ValueError("AzureWebJobsStorage connection string not found in environment variables.")
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = "spotifycontainer"
container_client = blob_service_client.get_container_client(container_name)
    
# Create the container if it doesn't exist
try:
    container_client.create_container()
    logging.info(f"Container '{container_name}' created.")
except Exception as e:
    logging.info(f"Container '{container_name}' may already exist: {e}")

    # Prepare blob path inside the virtual folder "raw_data/processed"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_path = f"raw_data/processed/spotify_data_{timestamp}.json"
    blob_client = container_client.get_blob_client(blob_path)

    # Convert the Spotify data to JSON format
    data_json = json.dumps(data, indent=2)
    
    # Upload the JSON data to the blob (overwrite if it exists)
    blob_client.upload_blob(data_json, overwrite=True)
    logging.info(f"Data successfully uploaded to blob: {blob_path}")

print("ended")     
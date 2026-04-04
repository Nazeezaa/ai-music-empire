"""YouTube Analytics Sync - Fetches channel stats and updates Firestore."""
import os
import json
import requests
from datetime import datetime

def get_youtube_credentials():
    """Get OAuth2 access token using refresh token."""
    client_id = os.environ.get("YOUTUBE_CLIENT_ID")
    client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET")
    refresh_token = os.environ.get("YOUTUBE_REFRESH_TOKEN")
    
    if not all([client_id, client_secret, refresh_token]):
        print("WARNING: YouTube credentials not set, skipping analytics sync")
        return None
    
    response = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    })
    
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"Failed to get access token: {response.text}")
        return None

def get_channel_stats(access_token, channel_id):
    """Fetch channel statistics from YouTube Data API v3."""
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "statistics,snippet",
        "id": channel_id,
        "access_token": access_token
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        items = response.json().get("items", [])
        if items:
            stats = items[0]["statistics"]
            return {
                "subscribers": int(stats.get("subscriberCount", 0)),
                "views": int(stats.get("viewCount", 0)),
                "videos": int(stats.get("videoCount", 0)),
            }
    print(f"Failed to get stats for channel {channel_id}: {response.text}")
    return None

def sync_all_channels():
    """Sync all 4 channel stats to Firestore."""
    import firebase_admin
    from firebase_admin import credentials, firestore
    
    # Initialize Firebase if not already done
    if not firebase_admin._apps:
        service_account = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
        if service_account:
            cred_dict = json.loads(service_account)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        else:
            print("WARNING: FIREBASE_SERVICE_ACCOUNT not set")
            return
    
    db = firestore.client()
    access_token = get_youtube_credentials()
    
    if not access_token:
        return
    
    # Channel mapping: Firestore doc ID -> YouTube channel ID
    # TODO: Update these with actual YouTube channel IDs
    channels = {
        "lofi_barista": {"youtube_id": "", "name": "Lofi Barista"},
        "rain_walker": {"youtube_id": "", "name": "Rain Walker"},
        "velvet_groove": {"youtube_id": "", "name": "Velvet Groove"},
        "piano_ghost": {"youtube_id": "", "name": "Piano Ghost"},
    }
    
    for doc_id, channel_info in channels.items():
        channel_id = channel_info["youtube_id"]
        if not channel_id:
            print(f"Skipping {channel_info['name']} - no YouTube channel ID configured")
            continue
            
        stats = get_channel_stats(access_token, channel_id)
        if stats:
            stats["last_synced"] = datetime.utcnow().isoformat()
            stats["status"] = "online"
            db.collection("channels").document(doc_id).update(stats)
            print(f"Updated {channel_info['name']}: {stats['subscribers']} subs, {stats['views']} views, {stats['videos']} videos")
        else:
            print(f"Failed to sync {channel_info['name']}")
    
    print("YouTube Analytics sync complete!")

if __name__ == "__main__":
    sync_all_channels()

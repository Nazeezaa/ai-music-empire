"""
upload_youtube.py - Upload processed tracks to YouTube via API
"""
import os
import sys
import json
import logging
from datetime import datetime
import requests
import yaml

logger = logging.getLogger(__name__)


def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def get_youtube_credentials(config):
    yt = config["youtube"]
    client_id = os.environ.get(yt["client_id_env"])
    client_secret = os.environ.get(yt["client_secret_env"])
    refresh_token = os.environ.get(yt["refresh_token_env"])
    missing = []
    if not client_id: missing.append(yt["client_id_env"])
    if not client_secret: missing.append(yt["client_secret_env"])
    if not refresh_token: missing.append(yt["refresh_token_env"])
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")
    return client_id, client_secret, refresh_token


def get_access_token(config):
    yt = config["youtube"]
    client_id, client_secret, refresh_token = get_youtube_credentials(config)
    response = requests.post(yt["token_uri"], data={
        "client_id": client_id, "client_secret": client_secret,
        "refresh_token": refresh_token, "grant_type": "refresh_token"
    }, timeout=30)
    response.raise_for_status()
    token_data = response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise ValueError(f"No access_token in response")
    logger.info("Obtained YouTube access token")
    return access_token


def build_metadata(config, genre=None, mood=None):
    content = config["content"]
    yt = config["youtube"]
    genre = genre or config["kie"]["default_genre"]
    mood = mood or config["kie"]["default_mood"]
    date_str = datetime.now().strftime("%Y-%m-%d")
    title = content["title_template"].format(genre=genre.title(), mood=mood.title(), date=date_str)
    description = content["description_template"].format(genre=genre, mood=mood)
    return {
        "snippet": {"title": title[:100], "description": description[:5000], "tags": content["tags"], "categoryId": str(yt["category_id"]), "defaultLanguage": yt.get("default_language", "en")},
        "status": {"privacyStatus": yt["privacy_status"], "selfDeclaredMadeForKids": False}
    }


def upload_to_youtube(audio_path, config=None, genre=None, mood=None):
    if config is None: config = load_config()
    if not os.path.exists(audio_path):
        logger.error(f"File not found: {audio_path}")
        return None
    access_token = get_access_token(config)
    metadata = build_metadata(config, genre, mood)
    logger.info(f"Uploading: {metadata['snippet']['title']}")
    upload_url = "https://www.googleapis.com/upload/youtube/v3/videos"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=UTF-8", "X-Upload-Content-Type": "video/mp4", "X-Upload-Content-Length": str(os.path.getsize(audio_path))}
    params = {"uploadType": "resumable", "part": "snippet,status"}
    try:
        init_response = requests.post(upload_url, headers=headers, params=params, json=metadata, timeout=30)
        init_response.raise_for_status()
        resumable_url = init_response.headers.get("Location")
        if not resumable_url:
            logger.error("No resumable upload URL")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Upload init failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response body: {e.response.text}")
        return None
    try:
        with open(audio_path, "rb") as f:
            upload_response = requests.put(resumable_url, headers={"Authorization": f"Bearer {access_token}", "Content-Type": "video/mp4"}, data=f, timeout=600)
            upload_response.raise_for_status()
        video_data = upload_response.json()
        video_id = video_data.get("id")
        if video_id:
            logger.info(f"Upload success! https://www.youtube.com/watch?v={video_id}")
            return video_id
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Upload failed: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python upload_youtube.py <audio_file>")
        sys.exit(1)
    config = load_config()
    video_id = upload_to_youtube(sys.argv[1], config)
    if video_id:
        print(f"SUCCESS: https://www.youtube.com/watch?v={video_id}")
    else:
        print("FAILED")
        sys.exit(1)

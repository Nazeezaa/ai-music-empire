"""
generate_music.py - Generate music tracks using KIE AI API
"""
import os
import sys
import time
import json
import logging
import requests
import yaml

logger = logging.getLogger(__name__)


def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def get_api_key(config):
    env_var = config["kie"]["api_key_env"]
    api_key = os.environ.get(env_var)
    if not api_key:
        raise ValueError(f"Environment variable '{env_var}' is not set.")
    return api_key


def generate_track(config, genre=None, mood=None, duration=None):
    kie_config = config["kie"]
    api_key = get_api_key(config)
    genre = genre or kie_config["default_genre"]
    mood = mood or kie_config["default_mood"]
    duration = duration or kie_config["default_duration"]
    base_url = kie_config["base_url"]
    logger.info(f"Generating: genre={genre}, mood={mood}, duration={duration}s")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "prompt": f"{genre} {mood} music, high quality",
        "customMode": False,
        "instrumental": True,
        "model": kie_config.get("model", "V4"),
        "callBackUrl": kie_config.get("callback_url", "https://example.com/callback")
    }
    try:
        response = requests.post(f"{base_url}/generate", headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        data = result.get("data", {})
        task_id = data.get("taskId") if isinstance(data, dict) else None
        if not task_id:
            logger.error(f"No task_id: {result}")
            return None
        logger.info(f"Started, task_id: {task_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed: {e}")
        return None
    retry_delay = config["pipeline"]["retry_delay"]
    for attempt in range(60):
        time.sleep(retry_delay)
        try:
            sr = requests.get(f"{base_url}/generate/record-info", headers=headers, params={"taskId": task_id}, timeout=30)
            sr.raise_for_status()
            sd = sr.json()
            task_data = sd.get("data", {})
            status = task_data.get("status", "").upper() if isinstance(task_data, dict) else ""
            if status == "SUCCESS":
                suno_data = task_data.get("response", {}).get("sunoData", [])
                if suno_data and len(suno_data) > 0:
                    audio_url = suno_data[0].get("audioUrl")
                    if audio_url:
                        return download_audio(audio_url, config, genre, mood)
                logger.error(f"No audio URL in response: {sd}")
                return None
            elif status in ("FAILED", "ERROR"):
                logger.error(f"Failed: {sd}")
                return None
            else:
                logger.info(f"Status: {status} ({attempt+1}/60)")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Check failed: {e}")
    return None


def generate_multiple_tracks(config, count=None):
    """Generate multiple tracks for long-form video concatenation.

    Args:
        config: Pipeline configuration dict.
        count: Number of tracks to generate. Defaults to config tracks_per_video or 30.

    Returns:
        List of audio file paths (skips failed generations).
    """
    if count is None:
        count = config["pipeline"].get("tracks_per_video", 30)
    logger.info(f"Generating {count} tracks for long-form video...")
    audio_files = []
    for i in range(count):
        logger.info(f"Generating track {i+1}/{count}...")
        audio_path = generate_track(config)
        if audio_path:
            audio_files.append(audio_path)
            logger.info(f"Track {i+1}/{count} complete: {audio_path}")
        else:
            logger.warning(f"Track {i+1}/{count} failed, skipping")
    logger.info(f"Generated {len(audio_files)}/{count} tracks successfully")
    return audio_files


def download_audio(url, config, genre, mood):
    output_dir = config["pipeline"]["output_dir"]
    os.makedirs(output_dir, exist_ok=True)
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"{genre}_{mood}_{ts}.mp3")
    try:
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded: {filepath} ({os.path.getsize(filepath)/1024/1024:.1f} MB)")
        return filepath
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    tracks_per_video = config["pipeline"].get("tracks_per_video", 30)
    if tracks_per_video > 1:
        results = generate_multiple_tracks(config)
        if results:
            print(f"SUCCESS: Generated {len(results)} tracks")
            for path in results:
                print(f"  {path}")
        else:
            print("FAILED: No tracks generated")
            sys.exit(1)
    else:
        result = generate_track(config)
        if result:
            print(f"SUCCESS: {result}")
        else:
            print("FAILED")
            sys.exit(1)

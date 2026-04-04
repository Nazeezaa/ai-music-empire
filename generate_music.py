"""
generate_music.py - Generate music tracks using Suno AI API
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


def get_suno_cookie(config):
    """Read the SUNO_COOKIE value from the environment."""
    env_var = config["suno"]["api_key_env"]
    cookie = os.environ.get(env_var)
    if not cookie:
        raise ValueError(f"Environment variable '{env_var}' is not set.")
    return cookie


def get_jwt(suno_cookie, config):
    """Exchange the Suno __client cookie for a fresh Clerk JWT."""
    clerk_url = config["suno"]["clerk_url"]
    resp = requests.get(
        clerk_url,
        headers={"Cookie": f"__client={suno_cookie}"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    # Clerk returns the JWT inside response -> sessions[0] -> last_active_token -> jwt
    try:
        sessions = data["response"]["sessions"]
        jwt = sessions[0]["last_active_token"]["jwt"]
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Failed to extract JWT from Clerk response: {e}")
        logger.debug(f"Clerk response: {json.dumps(data, indent=2)}")
        raise ValueError("Could not obtain JWT from Clerk. Is SUNO_COOKIE valid?")
    logger.info("Obtained fresh JWT from Clerk")
    return jwt


def generate_track(config, genre=None, mood=None, duration=None):
    """Generate a single music track via Suno API.

    Steps:
      1. Get JWT from Clerk using SUNO_COOKIE
      2. POST to Suno generate/v2 endpoint
      3. Poll the feed endpoint until the clip is ready
      4. Download the MP3
    """
    suno_config = config["suno"]
    suno_cookie = get_suno_cookie(config)
    jwt = get_jwt(suno_cookie, config)

    genre = genre or suno_config["default_genre"]
    mood = mood or suno_config["default_mood"]
    duration = duration or suno_config["default_duration"]
    base_url = suno_config["base_url"]

    logger.info(f"Generating: genre={genre}, mood={mood}, duration={duration}s")

    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
    }

    payload = {
        "gpt_description_prompt": f"{genre} {mood} music, high quality",
        "make_instrumental": True,
        "mv": "chirp-v4",
    }

    # Step 2 -- submit generation request
    try:
        response = requests.post(
            f"{base_url}/generate/v2/",
            headers=headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()
        clips = result.get("clips", [])
        if not clips:
            logger.error(f"No clips returned: {result}")
            return None
        clip_id = clips[0].get("id")
        if not clip_id:
            logger.error(f"No clip id in response: {result}")
            return None
        logger.info(f"Generation started, clip_id: {clip_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Generation request failed: {e}")
        return None

    # Step 3 -- poll feed endpoint for completion
    retry_delay = config["pipeline"]["retry_delay"]
    for attempt in range(60):
        time.sleep(retry_delay)
        try:
            feed_resp = requests.get(
                f"{base_url}/feed/{clip_id}",
                headers=headers,
                timeout=30,
            )
            feed_resp.raise_for_status()
            feed_data = feed_resp.json()

            # feed_data may be a list or a single object
            clip_data = feed_data[0] if isinstance(feed_data, list) else feed_data
            status = clip_data.get("status", "").lower()

            if status == "complete":
                audio_url = clip_data.get("audio_url")
                if audio_url:
                    return download_audio(audio_url, config, genre, mood)
                logger.error(f"Complete but no audio_url: {clip_data}")
                return None
            elif status in ("error", "failed"):
                logger.error(f"Generation failed: {clip_data}")
                return None
            else:
                logger.info(f"Status: {status} ({attempt + 1}/60)")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Poll failed: {e}")

    logger.error("Timed out waiting for clip to complete")
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
        logger.info(f"Generating track {i + 1}/{count}...")
        audio_path = generate_track(config)
        if audio_path:
            audio_files.append(audio_path)
            logger.info(f"Track {i + 1}/{count} complete: {audio_path}")
        else:
            logger.warning(f"Track {i + 1}/{count} failed, skipping")
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
        logger.info(f"Downloaded: {filepath} ({os.path.getsize(filepath) / 1024 / 1024:.1f} MB)")
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

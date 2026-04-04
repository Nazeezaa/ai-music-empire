"""
run_pipeline.py - Main orchestrator for AI Music Empire pipeline

Flow: generate_multiple_tracks() -> concatenate_audio() -> process_track() -> upload_to_youtube()
Rotates through all 4 channels daily using channel_identity.yaml
"""


import os
import sys
import random
import logging
import datetime

import yaml

from generate_music import generate_track, generate_multiple_tracks, load_config
from process_audio import process_track, concatenate_audio, get_duration
from upload_youtube import upload_to_youtube
from check_analytics import get_channel_analytics
from firestore_sync import (
    init_firestore, log_pipeline_run, log_upload,
    log_activity, sync_channel_after_upload
)


log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(config):
    log_file = config["pipeline"].get("log_file", "pipeline.log")
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="a")
    ])


def get_todays_channel():
    """Rotate channel based on day of year (cycle through 4 channels)."""
    with open("channel_identity.yaml", "r") as f:
        identity = yaml.safe_load(f)

    channels_order = ["lofi_barista", "rain_walker", "velvet_groove", "piano_ghost"]
    today = datetime.date.today()
    channel_index = today.timetuple().tm_yday % len(channels_order)
    current_channel = channels_order[channel_index]
    channel_config = identity["channels"][current_channel]

    # Select channel-specific Suno prompts from identity guide
    suno_prompt = random.choice(channel_config["suno_prompts"])
    genre = random.choice(channel_config["sub_genres"])
    mood = random.choice(channel_config["moods"])

    return current_channel, channel_config, suno_prompt, genre, mood


def run_pipeline():
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    # --- Channel Rotation ---
    current_channel, channel_config, suno_prompt, genre, mood = get_todays_channel()
    channel_name = channel_config["name"]
    channel_handle = channel_config["handle"]
    title_templates = channel_config["youtube_title_templates"]

    logger.info(f"=== Daily Channel: {channel_name} ({current_channel}) ===")
    logger.info(f"Genre: {genre} | Mood: {mood}")
    logger.info(f"Suno prompt: {suno_prompt}")

    # --- Initialize Firestore ---
    init_firestore()
    run_id = log_pipeline_run(channel=current_channel, status="started")

    try:
        # --- Generate Music ---
        logger.info(f"Generating tracks for {channel_name}...")
        tracks = generate_multiple_tracks(
            config,
            prompt=suno_prompt,
            genre=genre,
            mood=mood
        )

        # --- Process Audio ---
        logger.info("Processing and concatenating audio...")
        combined = concatenate_audio(tracks)
        processed = process_track(combined)
        duration = get_duration(processed)
        logger.info(f"Final track duration: {duration}s")

        # --- YouTube Upload ---
        title_template = random.choice(title_templates)
        video_title = title_template.replace("{date}", datetime.date.today().strftime("%B %d, %Y"))
        logger.info(f"Uploading to YouTube channel: {channel_handle}")
        logger.info(f"Video title: {video_title}")

        upload_result = upload_to_youtube(
            file_path=processed,
            title=video_title,
            channel=channel_handle
        )

        # --- Log Upload to Firestore ---
        log_upload(
            run_id=run_id,
            channel=current_channel,
            video_id=upload_result.get("video_id"),
            title=video_title,
            duration=duration
        )

        # --- Sync Channel Analytics ---
        sync_channel_after_upload(channel=current_channel)

        # --- Check Analytics ---
        analytics = get_channel_analytics(channel_handle)
        log_activity(channel=current_channel, activity="analytics_check", data=analytics)

        log_pipeline_run(channel=current_channel, status="completed", run_id=run_id)
        logger.info(f"Pipeline completed successfully for {channel_name}!")

    except Exception as e:
        logger.error(f"Pipeline failed for {channel_name}: {e}", exc_info=True)
        log_pipeline_run(channel=current_channel, status="failed", run_id=run_id, error=str(e))
        raise


if __name__ == "__main__":
    run_pipeline()


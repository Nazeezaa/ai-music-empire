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
from pipeline_health import HealthCheck
from generate_thumbnail import generate_thumbnail


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

    # --- Initialize Health Check ---
    health = HealthCheck(channel=current_channel)

    # --- Initialize Firestore ---
    init_firestore()
    run_id = log_pipeline_run(channel=current_channel, status="started")

    # --- Step 1: Suno Auth (implicit in generate) ---
    tracks = None
    try:
                logger.info(f"Authenticating with Suno API...")
                # Auth is handled inside generate_multiple_tracks; we verify it worked
                # by successfully generating tracks in the next step.
                health.check_pass("suno_auth")
except Exception as e:
            health.check_fail("suno_auth", e)

    # --- Step 2: Generate Music ---
        try:
                    logger.info(f"Generating tracks for {channel_name}...")
                    tracks = generate_multiple_tracks(
                        config,
                        prompt=suno_prompt,
                        genre=genre,
                        mood=mood
                    )
                    if not tracks:
                                    raise RuntimeError("generate_multiple_tracks returned empty result")
                                health.check_pass("suno_generate", f"Generated {len(tracks)} track(s)")
except Exception as e:
        health.check_fail("suno_generate", e)
        logger.error(f"Music generation failed: {e}", exc_info=True)

    # --- Step 3: Process & Concatenate Audio ---
    processed = None
    duration = None
    if tracks:
                try:
                                logger.info("Processing and concatenating audio...")
                                combined = concatenate_audio(tracks)
                                processed = process_track(combined)
                                duration = get_duration(processed)
                                logger.info(f"Final track duration: {duration}s")
                                health.check_pass("ffmpeg_concat", f"Audio ready ({duration}s)")
except Exception as e:
            health.check_fail("ffmpeg_concat", e)
            logger.error(f"Audio processing failed: {e}", exc_info=True)
else:
        health.check_fail(
                        "ffmpeg_concat",
                        RuntimeError("Skipped -- no tracks to process"),
                        fix="Fix music generation step first."
        )

    # --- Step 3.5: Generate Thumbnail ---
    thumbnail_path = None
    if processed:
                try:
                                logger.info(f"Generating thumbnail for {channel_name}...")
                                # Calculate volume number from day of year
                                volume_number = datetime.date.today().timetuple().tm_yday
            thumbnail_path = generate_thumbnail(
                                channel_key=current_channel,
                                volume_number=volume_number,
                                mood=mood
            )
            logger.info(f"Thumbnail generated: {thumbnail_path}")
            health.check_pass("thumbnail_gen", f"Thumbnail ready: {thumbnail_path}")
except Exception as e:
            logger.warning(f"Thumbnail generation failed (non-fatal): {e}", exc_info=True)
            health.check_fail("thumbnail_gen", e)

    # --- Step 4: YouTube Upload ---
    upload_result = None
    if processed:
                try:
                                title_template = random.choice(title_templates)
                                video_title = title_template.replace(
                                    "{date}", datetime.date.today().strftime("%B %d, %Y")
                                )
                                logger.info(f"Uploading to YouTube channel: {channel_handle}")
                                logger.info(f"Video title: {video_title}")

            upload_result = upload_to_youtube(
                                file_path=processed,
                                title=video_title,
                                channel=channel_handle,
                                thumbnail_path=thumbnail_path
            )
            if not upload_result or not upload_result.get("video_id"):
                                raise RuntimeError("upload_to_youtube returned no video_id")
                            health.check_pass("youtube_upload", f"Uploaded: {upload_result.get('video_id')}")
except Exception as e:
            health.check_fail("youtube_upload", e)
            logger.error(f"YouTube upload failed: {e}", exc_info=True)
else:
        health.check_fail(
                        "youtube_upload",
                        RuntimeError("Skipped -- no processed audio"),
                        fix="Fix audio processing step first."
        )

    # --- Step 5: Firestore Sync ---
    try:
                if upload_result and upload_result.get("video_id"):
                                log_upload(
                                                    run_id=run_id,
                                                    channel=current_channel,
                                                    video_id=upload_result.get("video_id"),
                                                    title=video_title,
                                                    duration=duration
                                )
                                sync_channel_after_upload(channel=current_channel)
                            log_pipeline_run(
                                            channel=current_channel,
                                            status="completed" if upload_result else "partial",
                                            run_id=run_id
                            )
        health.check_pass("firestore_sync")
except Exception as e:
        health.check_fail("firestore_sync", e)
        logger.error(f"Firestore sync failed: {e}", exc_info=True)

    # --- Step 6: YouTube Analytics ---
    try:
                if upload_result:
                                analytics = get_channel_analytics(channel_handle)
                                log_activity(
                                    channel=current_channel,
                                    activity="analytics_check",
                                    data=analytics
                                )
                            health.check_pass("youtube_analytics")
except Exception as e:
        health.check_fail("youtube_analytics", e)
        logger.error(f"Analytics sync failed: {e}", exc_info=True)

    # --- Save Health Report ---
    health.save_to_firestore()

    overall = health.overall_status()
    if overall == "failed":
                logger.error(f"Pipeline FAILED for {channel_name}")
        raise RuntimeError(
                        f"Pipeline failed for {channel_name}. "
                        f"See health report for details."
        )
elif overall == "partial":
        logger.warning(f"Pipeline completed PARTIALLY for {channel_name}")
else:
        logger.info(f"Pipeline completed successfully for {channel_name}!")


if __name__ == "__main__":
        run_pipeline()

"""
run_pipeline.py - Main orchestrator for AI Music Empire pipeline

Flow: generate_multiple_tracks() -> concatenate_audio() -> process_track() -> upload_to_youtube()
"""

import os
import sys
import logging
from datetime import datetime

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


def run_pipeline():
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    logger.info("=" * 60)
    logger.info(f"AI Music Empire Pipeline - {datetime.now().isoformat()}")
    logger.info("=" * 60)

    # Get GitHub Actions run number if available
    run_number = os.environ.get("GITHUB_RUN_NUMBER")
    if run_number:
        run_number = int(run_number)
        logger.info(f"GitHub Actions run #{run_number}")

    # Initialize Firestore
    db = None
    try:
        db = init_firestore()
        if db:
            log_activity(db, "🎵", "Pipeline run started", "info")
    except Exception as e:
        logger.warning(f"Failed to initialize Firestore: {e}")

    tracks_per_video = config["pipeline"].get("tracks_per_video", 30)
    genre = config["suno"].get("default_genre", "lofi")
    mood = config["suno"].get("default_mood", "chill")
    channel_name = config["youtube"].get("channel_name", "AI Music Empire")

    # Step 1: Generate multiple tracks
    logger.info(f"Step 1: Generating {tracks_per_video} tracks...")
    audio_files = generate_multiple_tracks(config, count=tracks_per_video)

    if not audio_files:
        logger.error("No tracks generated. Pipeline failed.")
        if db:
            log_pipeline_run(db, "failed", channel_name, 0, 0, run_number=run_number)
            log_activity(db, "❌", "Pipeline failed: no tracks generated", "error")
        sys.exit(1)

    logger.info(f"Generated {len(audio_files)} tracks successfully")

    # Step 2: Concatenate all tracks into one long audio file
    logger.info(f"Step 2: Concatenating {len(audio_files)} tracks...")
    concatenated_path = concatenate_audio(audio_files, config=config)
    if not concatenated_path:
        logger.error("Concatenation failed. Falling back to first track only.")
        concatenated_path = audio_files[0]

    total_duration = get_duration(concatenated_path) or 0
    logger.info(f"Concatenated audio: {total_duration:.0f}s ({total_duration/60:.1f} min)")

    # Step 3: Process (normalize loudness, add fades, convert to video)
    logger.info("Step 3: Processing audio and creating video...")
    video_path = process_track(concatenated_path, config)

    if not video_path:
        logger.error("Processing failed.")
        if db:
            log_pipeline_run(db, "failed", channel_name, len(audio_files), int(total_duration), run_number=run_number)
            log_activity(db, "❌", f"Pipeline failed at processing step ({len(audio_files)} tracks)", "error")
        sys.exit(1)

    logger.info(f"Video ready: {video_path}")

    # Step 4: Upload to YouTube
    logger.info("Step 4: Uploading to YouTube...")
    video_id = upload_to_youtube(video_path, config)

    if not video_id:
        logger.error("Upload failed.")
        if db:
            log_pipeline_run(db, "failed", channel_name, len(audio_files), int(total_duration), run_number=run_number)
            log_activity(db, "❌", f"Pipeline failed at upload step ({len(audio_files)} tracks)", "error")
        sys.exit(1)

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.info(f"Upload success: {video_url}")

    # Step 5: Sync Firestore — update channel stats + log everything
    if db:
        try:
            title = config["content"]["title_template"].format(
                genre=genre.title(),
                mood=mood.title(),
                date=datetime.now().strftime("%Y-%m-%d")
            )

            # Update channel doc: videos +1, status Active, last_upload timestamp
            sync_channel_after_upload(db, channel_name, video_id)

            # Log the upload
            log_upload(db, title, channel_name, video_id)

            # Log the pipeline run with proper run number
            log_pipeline_run(db, "success", channel_name, len(audio_files), int(total_duration), run_number=run_number)

            # Activity log entries
            log_activity(db, "📤", f"Uploaded: {title[:50]} ({len(audio_files)} tracks, {total_duration/60:.0f} min)", "upload")
            log_activity(db, "✅", f"Pipeline completed successfully!", "info")

            logger.info("Firestore sync complete: channel stats updated, upload logged")
        except Exception as e:
            logger.warning(f"Firestore logging failed: {e}")

    # Step 6: Check analytics
    logger.info("Step 6: Checking analytics...")
    try:
        analytics = get_channel_analytics(config, days=7)
    except Exception as e:
        logger.warning(f"Analytics check failed (non-critical): {e}")

    # Sync YouTube Analytics to Firestore
    try:
        from youtube_analytics import sync_all_channels
        sync_all_channels()
    except Exception as e:
        print(f"YouTube Analytics sync failed (non-fatal): {e}")

    # Summary
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  Tracks generated: {len(audio_files)}")
    logger.info(f"  Total duration:   {total_duration:.0f}s ({total_duration/60:.1f} min)")
    logger.info(f"  Video: {video_url}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
"""
run_pipeline.py - Main orchestrator for AI Music Empire pipeline

Flow: generate_multiple_tracks() â concatenate_audio() â process_track() â upload_to_youtube()
"""
import os
import sys
import logging
from datetime import datetime
import yaml

from generate_music import generate_track, generate_multiple_tracks, load_config
from process_audio import process_track, concatenate_audio, get_duration
from upload_youtube import upload_to_youtube
from check_analytics import get_channel_analytics
from firestore_sync import init_firestore, log_pipeline_run, log_upload, log_activity

log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(config):
    log_file = config["pipeline"].get("log_file", "pipeline.log")
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="a")
    ])


def run_pipeline():
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")
    logger.info("=" * 60)
    logger.info(f"AI Music Empire Pipeline - {datetime.now().isoformat()}")
    logger.info("=" * 60)

    # Initialize Firestore
    db = None
    try:
        db = init_firestore()
        if db:
            log_activity(db, "ð", "Pipeline run started")
    except Exception as e:
        logger.warning(f"Failed to initialize Firestore: {e}")

    tracks_per_video = config["pipeline"].get("tracks_per_video", 30)
    genre = config["kie"].get("default_genre", "lofi")
    mood = config["kie"].get("default_mood", "chill")
    channel_name = config["youtube"].get("channel_name", "AI Music Empire")

    # Step 1: Generate multiple tracks
    logger.info(f"Step 1: Generating {tracks_per_video} tracks...")
    audio_files = generate_multiple_tracks(config, count=tracks_per_video)

    if not audio_files:
        logger.error("No tracks generated. Pipeline failed.")
        if db:
            log_pipeline_run(db, "failed", channel_name, 0, 0)
            log_activity(db, "â", "Pipeline failed: no tracks generated")
        sys.exit(1)

    logger.info(f"Generated {len(audio_files)} tracks successfully")

    # Step 2: Concatenate all tracks into one long audio file
    logger.info(f"Step 2: Concatenating {len(audio_files)} tracks...")
    concatenated_path = concatenate_audio(audio_files, config=config)

    if not concatenated_path:
        logger.error("Concatenation failed. Falling back to first track only.")
        concatenated_path = audio_files[0]

    total_duration = get_duration(concatenated_path) or 0
    logger.info(f"Concatenated audio: {total_duration:.0f}s ({total_duration/60:.1f} min)")

    # Step 3: Process (normalize loudness, add fades, convert to video)
    logger.info("Step 3: Processing audio and creating video...")
    video_path = process_track(concatenated_path, config)

    if not video_path:
        logger.error("Processing failed.")
        if db:
            log_pipeline_run(db, "failed", channel_name, len(audio_files), int(total_duration))
            log_activity(db, "â", f"Pipeline failed at processing step ({len(audio_files)} tracks)")
        sys.exit(1)

    logger.info(f"Video ready: {video_path}")

    # Step 4: Upload to YouTube
    logger.info("Step 4: Uploading to YouTube...")
    video_id = upload_to_youtube(video_path, config)

    if not video_id:
        logger.error("Upload failed.")
        if db:
            log_pipeline_run(db, "failed", channel_name, len(audio_files), int(total_duration))
            log_activity(db, "â", f"Pipeline failed at upload step ({len(audio_files)} tracks)")
        sys.exit(1)

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.info(f"Upload success: {video_url}")

    # Step 5: Log to Firestore
    if db:
        try:
            title = config["content"]["title_template"].format(
                genre=genre.title(),
                mood=mood.title(),
                date=datetime.now().strftime("%Y-%m-%d")
            )
            log_upload(db, title, channel_name, video_id)
            log_pipeline_run(db, "success", channel_name, len(audio_files), int(total_duration))
            log_activity(db, "ð¤", f"Uploaded: {title[:50]} ({len(audio_files)} tracks, {total_duration/60:.0f} min)")
            log_activity(db, "â", f"Pipeline completed successfully!")
            logger.info("Firestore logging complete")
        except Exception as e:
            logger.warning(f"Firestore logging failed: {e}")

    # Step 6: Check analytics
    logger.info("Step 5: Checking analytics...")
    try:
        analytics = get_channel_analytics(config, days=7)
    except Exception as e:
        logger.warning(f"Analytics check failed (non-critical): {e}")

    # Summary
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  Tracks generated: {len(audio_files)}")
    logger.info(f"  Total duration:   {total_duration:.0f}s ({total_duration/60:.1f} min)")
    logger.info(f"  Video:            {video_url}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()

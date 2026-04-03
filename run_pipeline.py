"""
run_pipeline.py - Main orchestrator for AI Music Empire pipeline
"""
import os
import sys
import logging
from datetime import datetime
import yaml

from generate_music import generate_track, load_config
from process_audio import process_track
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
    except Exception as e:
        logger.warning(f"Failed to initialize Firestore: {e}")

    tracks_per_run = config["pipeline"].get("tracks_per_run", 1)
    results = []
    for i in range(tracks_per_run):
        logger.info(f"--- Track {i+1}/{tracks_per_run} ---")
        logger.info("Step 1: Generating music...")
        audio_path = generate_track(config)
        if not audio_path:
            logger.error("Generation failed")
            results.append({"track": i+1, "status": "failed", "step": "generate"})
            continue
        logger.info("Step 2: Processing audio...")
        processed_path = process_track(audio_path, config)
        if not processed_path:
            logger.warning("Processing failed, using raw audio")
            processed_path = audio_path
        logger.info("Step 3: Uploading to YouTube...")
        video_id = upload_to_youtube(processed_path, config)
        if not video_id:
            logger.error("Upload failed")
            results.append({"track": i+1, "status": "failed", "step": "upload"})
            continue
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        logger.info(f"Uploaded: {video_url}")
        results.append({"track": i+1, "status": "success", "video_id": video_id, "video_url": video_url})

        # Log upload to Firestore
        if db:
            try:
                channel_name = config["youtube"].get("channel_name", "AI Music Empire")
                title = config["content"]["title_template"].format(
                    genre=config["kie"].get("default_genre", "unknown"),
                    mood=config["kie"].get("default_mood", "unknown"),
                    date=datetime.now().strftime("%Y-%m-%d")
                )
                log_upload(db, title, channel_name, video_id)
                log_activity(db, "📤", f"Uploaded video: {title[:50]}")
            except Exception as e:
                logger.warning(f"Failed to log upload to Firestore: {e}")

    logger.info("Step 4: Checking analytics...")
    try:
        analytics = get_channel_analytics(config, days=7)
    except Exception as e:
        logger.warning(f"Analytics check failed: {e}")

    logger.info("=" * 60)
    success = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    logger.info(f"SUMMARY: {success} success, {failed} failed out of {len(results)}")
    for r in results:
        if r["status"] == "success":
            logger.info(f"  Track {r['track']}: {r['video_url']}")
        else:
            logger.info(f"  Track {r['track']}: FAILED at {r['step']}")

    # Log pipeline run to Firestore
    if db:
        try:
            channel_name = config["youtube"].get("channel_name", "AI Music Empire")
            run_status = "success" if failed == 0 else "failed"
            log_pipeline_run(db, run_status, channel_name, success, 0)
            log_activity(db, "✅" if run_status == "success" else "❌",
                        f"Pipeline run completed: {success} success, {failed} failed")
        except Exception as e:
            logger.warning(f"Failed to log pipeline run to Firestore: {e}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()

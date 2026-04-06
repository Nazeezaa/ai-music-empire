"""
run_pipeline.py - Main orchestrator for AI Music Empire pipeline

Flow: ai_producer.produce_session() -> generate_multiple_tracks() -> concatenate_audio()
      -> process_track() -> upload_to_youtube() -> analytics_agent

Rotates through all 4 channels daily using channel_identity.yaml
Uses AI Producer for data-driven prompt selection and metadata generation
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
    init_firestore,
    log_pipeline_run,
    log_upload,
    log_activity,
    sync_channel_after_upload
)
from pipeline_health import HealthCheck
from generate_thumbnail import generate_thumbnail

# AI Producer for smart, analytics-driven production decisions
try:
    from ai_producer import produce_session as ai_produce_session
    AI_PRODUCER_AVAILABLE = True
except ImportError:
    AI_PRODUCER_AVAILABLE = False

log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
def setup_logging(config):
    log_file = config["pipeline"].get("log_file", "pipeline.log")
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="a")
    ])


def load_recommendations():
    """
    Load AI-generated recommendations from recommendations.yaml to bias
    Suno prompt selection toward better-performing genres.

    Returns a dict of channel recommendations or empty dict on failure.
    """
    try:
        with open("recommendations.yaml", "r") as f:
            data = yaml.safe_load(f) or {}
        return data.get("recommendations", {})
    except FileNotFoundError:
        return {}
    except Exception as e:
        logging.getLogger("pipeline").warning(
            f"Could not load recommendations.yaml: {e}"
        )
        return {}


def get_todays_channel_slug():
    """Rotate channel based on day of year (cycle through 4 channels)."""
    channels_order = ["lofi_barista", "rain_walker", "velvet_groove", "piano_ghost"]
    today = datetime.date.today()
    channel_index = today.timetuple().tm_yday % len(channels_order)
    return channels_order[channel_index]
def get_todays_channel():
    """
    Legacy fallback: get channel config with random prompt selection.
    Used when AI Producer is not available.
    """
    with open("channel_identity.yaml", "r") as f:
        identity = yaml.safe_load(f)

    current_channel = get_todays_channel_slug()
    channel_config = identity["channels"][current_channel]

    # Load AI recommendations for prompt biasing
    recommendations = load_recommendations()
    display_key = current_channel.replace("_", "-")
    channel_recs = recommendations.get(display_key, {})

    # If analytics agent has a preferred genre, bias toward it
    suno_tuning = channel_recs.get("suno_prompt_tuning", {})
    preferred_genre = suno_tuning.get("preferred_genre", None)
    avoid_genre = suno_tuning.get("avoid_genre", None)

    # Select channel-specific Suno prompts from identity guide
    suno_prompts = channel_config["suno_prompts"]
    if preferred_genre:
        biased = [p for p in suno_prompts if preferred_genre.lower() in p.lower()]
        if biased:
            suno_prompt = random.choice(biased)
        else:
            suno_prompt = random.choice(suno_prompts)
    else:
        suno_prompt = random.choice(suno_prompts)

    # Filter sub_genres based on recommendations
    sub_genres = channel_config["sub_genres"]
    if avoid_genre:
        filtered = [g for g in sub_genres if g.lower() != avoid_genre.lower()]
        genre = random.choice(filtered) if filtered else random.choice(sub_genres)
    else:
        genre = random.choice(sub_genres)

    mood = random.choice(channel_config["moods"])
    return current_channel, channel_config, suno_prompt, genre, mood
def run_pipeline():
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    # --- Channel Selection ---
    current_channel = get_todays_channel_slug()
    channel_slug = current_channel.replace("_", "-")

    # --- AI Producer: Smart Session Planning ---
    session = None
    if AI_PRODUCER_AVAILABLE:
        try:
            logger.info(f"[AI Producer] Generating smart session for {channel_slug}...")
            session = ai_produce_session(channel_slug)
            logger.info(f"[AI Producer] Session ready: {session['title']}")
        except Exception as e:
            logger.warning(f"[AI Producer] Failed, falling back to legacy: {e}")
            session = None

    # Fallback to legacy channel selection if AI Producer unavailable
    if session:
        suno_prompt = session["suno_prompt"]
        genre = session["genres"][0] if session.get("genres") else "chill"
        mood = session["mood"]
        video_title = session["title"]
        video_description = session["description"]
        video_tags = session["tags"]
        thumbnail_style = session.get("thumbnail_style", {})
        volume_number = session.get("volume_number", 1)

        # Load channel_config for compatibility with existing steps
        with open("channel_identity.yaml", "r") as f:
            identity = yaml.safe_load(f)
        channel_config = identity["channels"][current_channel]
        channel_name = channel_config["name"]
        channel_handle = channel_config["handle"]
    else:
        # Legacy path
        current_channel, channel_config, suno_prompt, genre, mood = get_todays_channel()
        channel_name = channel_config["name"]
        channel_handle = channel_config["handle"]
        title_templates = channel_config["youtube_title_templates"]
        video_title = random.choice(title_templates).replace(
            "{date}", datetime.date.today().strftime("%B %d, %Y")
        )
        video_description = None
        video_tags = channel_config.get("tags", [])
        thumbnail_style = {}
        volume_number = datetime.date.today().timetuple().tm_yday

    logger.info(f"=== Daily Channel: {channel_name} ({current_channel}) ===")
    logger.info(f"Genre: {genre} | Mood: {mood}")
    logger.info(f"Suno prompt: {suno_prompt}")
    logger.info(f"Title: {video_title}")

    # --- Initialize Health Check ---
    health = HealthCheck(channel=current_channel)
    # --- Initialize Firestore ---
    db = None
    try:
        db = init_firestore()
        logger.info("Firestore initialized successfully.")
    except Exception as e:
        logger.warning(f"Firestore initialization failed (non-fatal): {e}")
        db = None

    if db is not None:
        try:
            log_pipeline_run(db, "started", current_channel)
        except Exception as e:
            logger.warning(f"Failed to log pipeline start to Firestore: {e}")

    # --- Step 1: Suno Auth (implicit in generate) ---
    tracks = None
    try:
        logger.info(f"Authenticating with Suno API...")
        health.check_pass("suno_auth")
    except Exception as e:
        health.check_fail("suno_auth", e)

    # --- Step 2: Generate Music ---
    try:
        logger.info(f"Generating tracks for {channel_name}...")
        tracks = generate_multiple_tracks(config)
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
        if db is not None and upload_result and upload_result.get("video_id"):
            log_upload(
                db,
                current_channel,
                upload_result.get("video_id"),
                video_title,
                duration=duration or 0
            )
            sync_channel_after_upload(db, current_channel)
            log_pipeline_run(
                db,
                "completed" if upload_result else "partial",
                current_channel,
                tracks_generated=len(tracks) if tracks else 0,
                video_duration=duration or 0
            )
            health.check_pass("firestore_sync")
        elif db is None:
            logger.warning("Skipping Firestore sync: Firestore not initialized.")
        else:
            if db is not None:
                log_pipeline_run(db, "partial", current_channel)
    except Exception as e:
        health.check_fail("firestore_sync", e)
        logger.error(f"Firestore sync failed: {e}", exc_info=True)
    # --- Step 6: YouTube Analytics ---
    try:
        if upload_result:
            analytics = get_channel_analytics(channel_handle)
            if db is not None:
                log_activity(
                    db,
                    "analytics_check",
                    channel=current_channel,
                    details=str(analytics)
                )
            health.check_pass("youtube_analytics")
    except Exception as e:
        health.check_fail("youtube_analytics", e)
        logger.error(f"Analytics sync failed: {e}", exc_info=True)

    # --- Step 7: AI Analytics Agent ---
    try:
        logger.info("Running AI Analytics Agent...")
        from analytics_agent import main as run_analytics_agent
        analytics_results = run_analytics_agent()
        if analytics_results:
            health.check_pass(
                "analytics_agent",
                f"Analyzed {len(analytics_results)} channels"
            )
            logger.info("AI Analytics Agent completed successfully.")
        else:
            health.check_pass("analytics_agent", "Completed (no data)")
    except Exception as e:
        # Analytics failure should NOT block the main pipeline
        logger.warning(f"AI Analytics Agent failed (non-fatal): {e}", exc_info=True)
        health.check_fail("analytics_agent", e)
    # --- Save Health Report ---
    health.save_to_firestore()

    overall = health.overall_status
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

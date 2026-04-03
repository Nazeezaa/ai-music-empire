"""
firestore_sync.py - Firebase Firestore integration for AI Music Empire
Handles all Firestore database operations for logging pipeline runs, uploads, and analytics
"""
import os
import json
import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

logger = logging.getLogger(__name__)

PROJECT_ID = "ai-music-empire-d9ab3"


def init_firestore():
    """
    Initialize Firebase Admin SDK and return Firestore client.
    Uses FIREBASE_SERVICE_ACCOUNT environment variable containing JSON credentials.

    Returns:
        firestore.Client: Firestore database client, or None if initialization fails
    """
    if not FIREBASE_AVAILABLE:
        logger.warning("firebase-admin is not installed. Firestore integration disabled.")
        return None

    try:
        service_account_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
        if not service_account_json:
            logger.warning("FIREBASE_SERVICE_ACCOUNT environment variable not set. Firestore integration disabled.")
            return None

        creds_dict = json.loads(service_account_json)
        creds = credentials.Certificate(creds_dict)

        if not firebase_admin._apps:
            firebase_admin.initialize_app(creds, {"projectId": PROJECT_ID})

        db = firestore.client()
        logger.info("Firestore client initialized successfully")
        return db
    except Exception as e:
        logger.error(f"Failed to initialize Firestore: {e}")
        return None


def _slugify(text: str) -> str:
    """Convert text to URL-friendly slug format."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    return text.strip('-')


def update_channel_stats(
    db,
    channel_id: str,
    name: str,
    subs: int,
    views: int,
    videos: int,
    status: str = 'active'
) -> bool:
    """
    Update or create a channel document in the 'channels' collection.

    Args:
        db: Firestore client
        channel_id: YouTube channel ID
        name: Channel display name
        subs: Subscriber count
        views: Total views count
        videos: Total videos count
        status: Channel status (default: 'active')

    Returns:
        bool: True if successful, False otherwise
    """
    if db is None:
        return False

    try:
        doc_id = _slugify(name)
        channel_data = {
            "channelId": channel_id,
            "name": name,
            "subscribers": subs,
            "views": views,
            "videos": videos,
            "status": status,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        db.collection("channels").document(doc_id).set(channel_data, merge=True)
        logger.info(f"Updated channel stats for {name}: {subs} subs, {views} views, {videos} videos")
        return True
    except Exception as e:
        logger.error(f"Failed to update channel stats: {e}")
        return False


def log_pipeline_run(
    db,
    status: str,
    channel: str,
    tracks_generated: int,
    video_duration: int,
    run_number: Optional[int] = None
) -> bool:
    """
    Add a new document to the 'pipeline_runs' collection.

    Args:
        db: Firestore client
        status: Run status ('success' or 'failed')
        channel: Channel name
        tracks_generated: Number of tracks generated
        video_duration: Total video duration in seconds
        run_number: Optional run number/ID

    Returns:
        bool: True if successful, False otherwise
    """
    if db is None:
        return False

    try:
        run_data = {
            "status": status,
            "channel": channel,
            "tracksGenerated": tracks_generated,
            "videoDuration": video_duration,
            "runNumber": run_number,
            "timestamp": firestore.SERVER_TIMESTAMP,
        }
        db.collection("pipeline_runs").add(run_data)
        logger.info(f"Logged pipeline run: status={status}, tracks={tracks_generated}")
        return True
    except Exception as e:
        logger.error(f"Failed to log pipeline run: {e}")
        return False


def log_upload(
    db,
    title: str,
    channel: str,
    video_id: str,
    thumbnail_url: Optional[str] = None
) -> bool:
    """
    Add a new document to the 'uploads' collection.

    Args:
        db: Firestore client
        title: Video title
        channel: Channel name
        video_id: YouTube video ID
        thumbnail_url: Optional thumbnail URL

    Returns:
        bool: True if successful, False otherwise
    """
    if db is None:
        return False

    try:
        upload_data = {
            "title": title,
            "channel": channel,
            "videoId": video_id,
            "videoUrl": f"https://www.youtube.com/watch?v={video_id}",
            "thumbnailUrl": thumbnail_url,
            "timestamp": firestore.SERVER_TIMESTAMP,
        }
        db.collection("uploads").add(upload_data)
        logger.info(f"Logged upload: {title} (video_id={video_id})")
        return True
    except Exception as e:
        logger.error(f"Failed to log upload: {e}")
        return False


def log_activity(db, icon: str, text: str) -> bool:
    """
    Add a new document to the 'activity_log' collection.

    Args:
        db: Firestore client
        icon: Icon/emoji to display
        text: Activity description text

    Returns:
        bool: True if successful, False otherwise
    """
    if db is None:
        return False

    try:
        activity_data = {
            "icon": icon,
            "text": text,
            "timestamp": firestore.SERVER_TIMESTAMP,
        }
        db.collection("activity_log").add(activity_data)
        logger.info(f"Logged activity: {text}")
        return True
    except Exception as e:
        logger.error(f"Failed to log activity: {e}")
        return False


def update_revenue(db, estimated: float, target: float = 50000) -> bool:
    """
    Update the 'revenue/current' document.

    Args:
        db: Firestore client
        estimated: Estimated revenue
        target: Revenue target (default: 50000)

    Returns:
        bool: True if successful, False otherwise
    """
    if db is None:
        return False

    try:
        revenue_data = {
            "estimated": estimated,
            "target": target,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        db.collection("revenue").document("current").set(revenue_data, merge=True)
        logger.info(f"Updated revenue: ${estimated} / ${target}")
        return True
    except Exception as e:
        logger.error(f"Failed to update revenue: {e}")
        return False


def update_channel_view_history(db, channel_doc_id: str, views: int) -> bool:
    """
    Append a view count to the channel's viewHistory array, keeping last 30 entries.

    Args:
        db: Firestore client
        channel_doc_id: Channel document ID (slugified name)
        views: Current view count

    Returns:
        bool: True if successful, False otherwise
    """
    if db is None:
        return False

    try:
        channel_ref = db.collection("channels").document(channel_doc_id)
        channel_doc = channel_ref.get()

        view_history = []
        if channel_doc.exists:
            view_history = channel_doc.get("viewHistory", [])

        view_entry = {
            "views": views,
            "timestamp": datetime.now().isoformat()
        }
        view_history.append(view_entry)

        if len(view_history) > 30:
            view_history = view_history[-30:]

        channel_ref.set({"viewHistory": view_history}, merge=True)
        logger.info(f"Updated view history for {channel_doc_id}: {views} views")
        return True
    except Exception as e:
        logger.error(f"Failed to update channel view history: {e}")
        return False


def seed_test_data(db) -> None:
    """
    Seed Firestore with initial test data for development/testing.

    Args:
        db: Firestore client
    """
    if db is None:
        logger.warning("Cannot seed test data: Firestore not initialized")
        return

    try:
        logger.info("Seeding test data...")

        channels = [
            {"name": "Lofi Hip Hop Beats", "channelId": "UCxxx1", "subs": 45000, "views": 3200000, "videos": 287},
            {"name": "Ambient Music Studio", "channelId": "UCxxx2", "subs": 28000, "views": 1850000, "videos": 156},
            {"name": "Jazz Piano Vibes", "channelId": "UCxxx3", "subs": 62000, "views": 5100000, "videos": 412},
            {"name": "Synthwave Dreams", "channelId": "UCxxx4", "subs": 38000, "views": 2400000, "videos": 203},
        ]

        for ch in channels:
            update_channel_stats(db, ch["channelId"], ch["name"], ch["subs"], ch["views"], ch["videos"])

        pipeline_runs = [
            {"status": "success", "channel": "Lofi Hip Hop Beats", "tracks": 2, "duration": 1200},
            {"status": "success", "channel": "Ambient Music Studio", "tracks": 1, "duration": 600},
            {"status": "failed", "channel": "Jazz Piano Vibes", "tracks": 0, "duration": 0},
            {"status": "success", "channel": "Synthwave Dreams", "tracks": 1, "duration": 900},
        ]

        for run in pipeline_runs:
            log_pipeline_run(db, run["status"], run["channel"], run["tracks"], run["duration"])

        uploads = [
            {"title": "Lofi Hip Hop - Late Night Study Mix", "channel": "Lofi Hip Hop Beats", "video_id": "abc123def456"},
            {"title": "Ambient Relaxation - Forest Sounds", "channel": "Ambient Music Studio", "video_id": "ghi789jkl012"},
            {"title": "Jazz Piano - Smooth Evening", "channel": "Jazz Piano Vibes", "video_id": "mno345pqr678"},
        ]

        for up in uploads:
            log_upload(db, up["title"], up["channel"], up["video_id"])

        activities = [
            {"icon": "🎵", "text": "Generated 2 tracks for Lofi Hip Hop channel"},
            {"icon": "📤", "text": "Uploaded video to Lofi Hip Hop Beats"},
            {"icon": "📊", "text": "Channel reached 45,000 subscribers"},
            {"icon": "⚠️", "text": "Jazz Piano pipeline run failed"},
            {"icon": "🎉", "text": "Total views exceeded 12 million"},
        ]

        for act in activities:
            log_activity(db, act["icon"], act["text"])

        update_revenue(db, estimated=12500.75, target=50000)

        update_channel_view_history(db, "lofi-hip-hop-beats", 3200000)
        update_channel_view_history(db, "ambient-music-studio", 1850000)

        logger.info("Test data seeding completed successfully")
    except Exception as e:
        logger.error(f"Failed to seed test data: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    db = init_firestore()
    if db:
        seed_test_data(db)
        print("Firestore sync initialized and test data seeded!")
    else:
        print("Failed to initialize Firestore. Check FIREBASE_SERVICE_ACCOUNT environment variable.")

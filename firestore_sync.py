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
    """Convert text to underscore-separated slug format matching dashboard doc IDs."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    return text.strip('_')


def update_channel_stats(
    db,
    channel_id: str,
    name: str,
    subs: int,
    views: int,
    videos: int,
    status: str = 'Active'
) -> bool:
    """
    Update or create a channel document in the 'channels' collection.
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


def sync_channel_after_upload(db, channel_name: str, video_id: str = None) -> bool:
    """
    Update channel doc after a successful YouTube upload.
    Increments video count by 1, sets status to 'Active', and records last_upload timestamp.
    """
    if db is None:
        return False

    try:
        doc_id = _slugify(channel_name)
        channel_ref = db.collection("channels").document(doc_id)
        channel_doc = channel_ref.get()

        current_videos = 0
        if channel_doc.exists:
            current_videos = channel_doc.get("videos") or 0

        update_data = {
            "videos": current_videos + 1,
            "status": "Active",
            "last_upload": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        if video_id:
            update_data["last_video_id"] = video_id
            update_data["last_video_url"] = f"https://www.youtube.com/watch?v={video_id}"

        channel_ref.set(update_data, merge=True)
        logger.info(f"Synced channel '{channel_name}' after upload: videos={current_videos + 1}, status=Active")
        return True
    except Exception as e:
        logger.error(f"Failed to sync channel after upload: {e}")
        return False


def log_pipeline_run(
    db,
    status: str,
    channel: str,
    tracks_generated: int,
    video_duration: int,
    run_number: Optional[int] = None,
    steps: Optional[Dict[str, str]] = None
) -> bool:
    """
    Add a new document to the 'pipeline_runs' collection.
    Uses 'started_at' field name to match dashboard listener.
    """
    if db is None:
        return False

    try:
        run_name = f"Pipeline #{run_number}" if run_number else f"Run {datetime.now().strftime('%Y%m%d-%H%M')}"
        run_data = {
            "status": status,
            "channel": channel,
            "tracksGenerated": tracks_generated,
            "videoDuration": video_duration,
            "run_number": run_number or int(datetime.now().strftime('%Y%m%d%H%M')),
            "run_name": run_name,
            "started_at": firestore.SERVER_TIMESTAMP,
            "steps": steps or {
                "generate": "Done" if tracks_generated > 0 else "Failed",
                "video": "Done" if status == "success" else "Failed",
                "thumbnail": "Done" if status == "success" else "Pending",
                "upload": "Done" if status == "success" else "Failed",
                "metadata": "Done" if status == "success" else "Pending",
            },
        }
        db.collection("pipeline_runs").add(run_data)
        logger.info(f"Logged pipeline run: {run_name}, status={status}, tracks={tracks_generated}")
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
    Uses 'uploaded_at' field name to match dashboard listener.
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
            "uploaded_at": firestore.SERVER_TIMESTAMP,
        }
        db.collection("uploads").add(upload_data)
        logger.info(f"Logged upload: {title} (video_id={video_id})")
        return True
    except Exception as e:
        logger.error(f"Failed to log upload: {e}")
        return False


def log_activity(db, icon: str, text: str, activity_type: str = "info") -> bool:
    """
    Add a new document to the 'activity_log' collection.
    Uses 'type' and 'message' field names to match dashboard listener.
    """
    if db is None:
        return False

    try:
        activity_data = {
            "type": activity_type,
            "message": text,
            "icon": icon,
            "timestamp": firestore.SERVER_TIMESTAMP,
        }
        db.collection("activity_log").add(activity_data)
        logger.info(f"Logged activity: {text}")
        return True
    except Exception as e:
        logger.error(f"Failed to log activity: {e}")
        return False


def update_revenue(db, estimated: float, target: float = 50000) -> bool:
    """Update the 'revenue/current' document."""
    if db is None:
        return False

    try:
        revenue_data = {
            "estimated": estimated,
            "total_thb": estimated,
            "target": target,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        db.collection("revenue").document("current").set(revenue_data, merge=True)
        logger.info(f"Updated revenue: {estimated} / {target}")
        return True
    except Exception as e:
        logger.error(f"Failed to update revenue: {e}")
        return False


def update_channel_view_history(db, channel_doc_id: str, views: int) -> bool:
    """Append a view count to the channel's viewHistory array, keeping last 30 entries."""
    if db is None:
        return False

    try:
        channel_ref = db.collection("channels").document(channel_doc_id)
        channel_doc = channel_ref.get()

        view_history = []
        if channel_doc.exists:
            view_history = channel_doc.get("viewHistory") or []

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


def seed_initial_channels(db) -> None:
    """Seed Firestore with the four AI Music Empire channels."""
    if db is None:
        return

    channels = [
        {"doc_id": "lofi_barista", "name": "Lofi Barista", "status": "Active"},
        {"doc_id": "rain_walker", "name": "Rain Walker", "status": "Setting Up"},
        {"doc_id": "velvet_groove", "name": "Velvet Groove", "status": "Setting Up"},
        {"doc_id": "piano_ghost", "name": "Piano Ghost", "status": "Setting Up"},
    ]
    for ch in channels:
        db.collection("channels").document(ch["doc_id"]).set({
            "name": ch["name"],
            "subscribers": 0,
            "views": 0,
            "videos": 0,
            "status": ch["status"],
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }, merge=True)
    logger.info("Seeded initial channel documents")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    db = init_firestore()
    if db:
        seed_initial_channels(db)
        print("Firestore sync initialized and channels seeded!")
    else:
        print("Failed to initialize Firestore. Check FIREBASE_SERVICE_ACCOUNT environment variable.")

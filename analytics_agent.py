"""
analytics_agent.py - AI Analytics Agent for AI Music Empire

Cross-references YouTube performance data with channel_identity.yaml to generate
actionable recommendations for Suno prompt tuning, thumbnail styles, upload timing,
and channel focus.

Classes:
    YouTubeDataFetcher  - Fetches channel stats, recent videos, analytics via YouTube APIs
    PatternAnalyzer     - Cross-references performance with channel identity genres
    RecommendationEngine - Generates actionable recommendations
    FirestoreWriter     - Writes reports to Firestore analytics_reports collection
"""

import os
import sys
import json
import logging
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Channel ID map (YouTube channel IDs for each brand)
# ---------------------------------------------------------------------------
CHANNEL_MAP = {
    "lofi-barista": "UCC76tzAG4JH8YYtehJAhwCg",
    "rain-walker": "UCTS92ipxcFAe3DIL4BYmycQ",
    "velvet-groove": "UCqDKZ3d6A3baCnmVp5JahJw",
    "piano-ghost": "UCEbUj2bHA6kzm_GKrEB-YTw",
}

# Internal key mapping (underscore <-> hyphen)
CHANNEL_KEY_MAP = {
    "lofi_barista": "lofi-barista",
    "rain_walker": "rain-walker",
    "velvet_groove": "velvet-groove",
    "piano_ghost": "piano-ghost",
}


# ===========================================================================
# YouTubeDataFetcher
# ===========================================================================
class YouTubeDataFetcher:
    """
    Fetches channel statistics, recent videos, and analytics data
    via YouTube Data API v3 and YouTube Analytics API v2.
    """

    def __init__(self):
        self.youtube = None
        self.youtube_analytics = None
        self._init_clients()

    def _init_clients(self):
        """Initialize YouTube API clients using environment credentials."""
        try:
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build

            client_id = os.environ.get("YOUTUBE_CLIENT_ID", "")
            client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET", "")
            refresh_token = os.environ.get("YOUTUBE_REFRESH_TOKEN", "")

            if not all([client_id, client_secret, refresh_token]):
                logger.warning("YouTube API credentials not fully configured.")
                return

            credentials = Credentials(
                token=None,
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                token_uri="https://oauth2.googleapis.com/token",
            )

            self.youtube = build("youtube", "v3", credentials=credentials)
            self.youtube_analytics = build(
                "youtubeAnalytics", "v2", credentials=credentials
            )
            logger.info("YouTube API clients initialized successfully.")
        except Exception as e:
            logger.warning(f"Failed to initialize YouTube API clients: {e}")

    def fetch_channel_stats(self, channel_id: str) -> Dict[str, Any]:
        """Fetch basic channel statistics (subscribers, views, videos)."""
        if not self.youtube:
            return self._mock_channel_stats(channel_id)
        try:
            response = (
                self.youtube.channels()
                .list(part="statistics,snippet", id=channel_id)
                .execute()
            )
            if response.get("items"):
                item = response["items"][0]
                stats = item["statistics"]
                return {
                    "channel_id": channel_id,
                    "title": item["snippet"]["title"],
                    "subscribers": int(stats.get("subscriberCount", 0)),
                    "total_views": int(stats.get("viewCount", 0)),
                    "video_count": int(stats.get("videoCount", 0)),
                    "fetched_at": datetime.utcnow().isoformat() + "Z",
                }
            return self._mock_channel_stats(channel_id)
        except Exception as e:
            logger.warning(f"Channel stats fetch failed for {channel_id}: {e}")
            return self._mock_channel_stats(channel_id)

    def fetch_recent_videos(self, channel_id: str, max_results: int = 10) -> List[Dict]:
        """Fetch most recent videos for a channel."""
        if not self.youtube:
            return self._mock_recent_videos(channel_id)
        try:
            response = (
                self.youtube.search()
                .list(
                    part="snippet",
                    channelId=channel_id,
                    order="date",
                    type="video",
                    maxResults=max_results,
                )
                .execute()
            )
            videos = []
            for item in response.get("items", []):
                video_id = item["id"]["videoId"]
                stats = self._fetch_video_stats(video_id)
                videos.append(
                    {
                        "video_id": video_id,
                        "title": item["snippet"]["title"],
                        "published_at": item["snippet"]["publishedAt"],
                        **stats,
                    }
                )
            return videos if videos else self._mock_recent_videos(channel_id)
        except Exception as e:
            logger.warning(f"Recent videos fetch failed for {channel_id}: {e}")
            return self._mock_recent_videos(channel_id)

    def _fetch_video_stats(self, video_id: str) -> Dict[str, Any]:
        """Fetch statistics for a single video."""
        try:
            response = (
                self.youtube.videos()
                .list(part="statistics,contentDetails", id=video_id)
                .execute()
            )
            if response.get("items"):
                stats = response["items"][0]["statistics"]
                return {
                    "views": int(stats.get("viewCount", 0)),
                    "likes": int(stats.get("likeCount", 0)),
                    "comments": int(stats.get("commentCount", 0)),
                }
        except Exception:
            pass
        return {"views": 0, "likes": 0, "comments": 0}

    def fetch_analytics(self, channel_id: str) -> Dict[str, Any]:
        """
        Fetch analytics data: views, watch time, CTR, retention,
        subscriber growth over the last 28 days.
        """
        if not self.youtube_analytics:
            return self._mock_analytics(channel_id)
        try:
            end_date = datetime.utcnow().strftime("%Y-%m-%d")
            start_date = (datetime.utcnow() - timedelta(days=28)).strftime("%Y-%m-%d")

            response = (
                self.youtube_analytics.reports()
                .query(
                    ids=f"channel=={channel_id}",
                    startDate=start_date,
                    endDate=end_date,
                    metrics="views,estimatedMinutesWatched,averageViewDuration,subscribersGained,subscribersLost",
                    dimensions="day",
                    sort="-day",
                )
                .execute()
            )
            rows = response.get("rows", [])
            total_views = sum(r[1] for r in rows) if rows else 0
            total_watch_min = sum(r[2] for r in rows) if rows else 0
            avg_duration = sum(r[3] for r in rows) / len(rows) if rows else 0
            subs_gained = sum(r[4] for r in rows) if rows else 0
            subs_lost = sum(r[5] for r in rows) if rows else 0

            return {
                "period": f"{start_date} to {end_date}",
                "total_views": total_views,
                "watch_time_minutes": total_watch_min,
                "avg_view_duration_seconds": round(avg_duration, 1),
                "ctr_percent": round(total_views / max(total_watch_min, 1) * 0.5, 2),
                "subscribers_gained": subs_gained,
                "subscribers_lost": subs_lost,
                "net_subscriber_growth": subs_gained - subs_lost,
                "retention_percent": round(
                    min(avg_duration / 180 * 100, 100), 1
                ),
            }
        except Exception as e:
            logger.warning(f"Analytics fetch failed for {channel_id}: {e}")
            return self._mock_analytics(channel_id)

    # -- Mock data fallbacks --------------------------------------------------

    @staticmethod
    def _mock_channel_stats(channel_id: str) -> Dict[str, Any]:
        return {
            "channel_id": channel_id,
            "title": "Unknown",
            "subscribers": 0,
            "total_views": 0,
            "video_count": 0,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "_mock": True,
        }

    @staticmethod
    def _mock_recent_videos(channel_id: str) -> List[Dict]:
        return [
            {
                "video_id": "mock",
                "title": "No data available",
                "published_at": datetime.utcnow().isoformat() + "Z",
                "views": 0,
                "likes": 0,
                "comments": 0,
                "_mock": True,
            }
        ]

    @staticmethod
    def _mock_analytics(channel_id: str) -> Dict[str, Any]:
        return {
            "period": "last 28 days",
            "total_views": 0,
            "watch_time_minutes": 0,
            "avg_view_duration_seconds": 0,
            "ctr_percent": 0,
            "subscribers_gained": 0,
            "subscribers_lost": 0,
            "net_subscriber_growth": 0,
            "retention_percent": 0,
            "_mock": True,
        }


# ===========================================================================
# PatternAnalyzer
# ===========================================================================
class PatternAnalyzer:
    """
    Cross-references video performance data with channel_identity.yaml
    to identify which genres, moods, and styles perform best.
    """

    def __init__(self, identity_path: str = "channel_identity.yaml"):
        self.identity = {}
        try:
            with open(identity_path, "r") as f:
                self.identity = yaml.safe_load(f) or {}
            logger.info(f"Loaded channel identity from {identity_path}")
        except FileNotFoundError:
            logger.warning(f"{identity_path} not found; pattern analysis limited.")
        except Exception as e:
            logger.warning(f"Failed to load {identity_path}: {e}")

    def analyze_genre_performance(
        self, channel_key: str, videos: List[Dict], analytics: Dict
    ) -> Dict[str, Any]:
        """
        Score each genre/sub-genre by matching video titles and performance.
        """
        channels = self.identity.get("channels", {})
        channel_cfg = channels.get(channel_key, {})
        sub_genres = channel_cfg.get("sub_genres", [])

        genre_scores: Dict[str, Dict[str, float]] = {}
        for genre in sub_genres:
            matching = [
                v for v in videos if genre.lower() in v.get("title", "").lower()
            ]
            total_views = sum(v.get("views", 0) for v in matching)
            avg_views = total_views / max(len(matching), 1)
            genre_scores[genre] = {
                "video_count": len(matching),
                "total_views": total_views,
                "avg_views": round(avg_views, 1),
                "score": round(avg_views * (1 + len(matching) * 0.1), 1),
            }

        sorted_genres = sorted(
            genre_scores.items(), key=lambda x: x[1]["score"], reverse=True
        )
        return {
            "channel": channel_key,
            "genre_scores": dict(sorted_genres),
            "top_genre": sorted_genres[0][0] if sorted_genres else "unknown",
            "bottom_genre": sorted_genres[-1][0] if sorted_genres else "unknown",
        }

    def analyze_growth_velocity(self, analytics: Dict) -> Dict[str, Any]:
        """Calculate subscriber growth velocity and trajectory."""
        net_growth = analytics.get("net_subscriber_growth", 0)
        total_views = analytics.get("total_views", 0)
        watch_time = analytics.get("watch_time_minutes", 0)

        growth_rate = net_growth / 28  # daily average
        views_per_sub = total_views / max(net_growth, 1)

        return {
            "daily_growth_rate": round(growth_rate, 2),
            "weekly_growth_rate": round(growth_rate * 7, 2),
            "views_per_subscriber": round(views_per_sub, 1),
            "watch_time_per_view": round(watch_time / max(total_views, 1), 2),
            "trajectory": (
                "accelerating" if growth_rate > 1
                else "steady" if growth_rate > 0
                else "declining"
            ),
        }

    def analyze_retention_patterns(self, analytics: Dict) -> Dict[str, Any]:
        """Analyze viewer retention and engagement quality."""
        avg_duration = analytics.get("avg_view_duration_seconds", 0)
        retention = analytics.get("retention_percent", 0)

        return {
            "avg_view_duration_seconds": avg_duration,
            "retention_percent": retention,
            "quality_tier": (
                "excellent" if retention >= 60
                else "good" if retention >= 40
                else "needs_improvement" if retention >= 20
                else "low"
            ),
            "recommendation": (
                "Viewers are highly engaged -- maintain current style"
                if retention >= 60
                else "Good retention -- experiment with longer intros"
                if retention >= 40
                else "Consider shorter tracks or stronger opening hooks"
                if retention >= 20
                else "Focus on first-30-second engagement and thumbnail CTR"
            ),
        }


# ===========================================================================
# RecommendationEngine
# ===========================================================================
class RecommendationEngine:
    """
    Generates actionable recommendations for Suno prompt tuning,
    thumbnail styles, upload timing, channel focus, and weekly summaries.
    """

    def generate_recommendations(
        self,
        channel_key: str,
        channel_stats: Dict,
        genre_analysis: Dict,
        growth: Dict,
        retention: Dict,
    ) -> Dict[str, Any]:
        """Generate a full recommendation report for one channel."""
        return {
            "channel": channel_key,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "suno_prompt_tuning": self._suno_recommendations(genre_analysis, retention),
            "thumbnail_style": self._thumbnail_recommendations(genre_analysis, growth),
            "upload_timing": self._timing_recommendations(growth),
            "channel_focus": self._focus_recommendations(
                genre_analysis, growth, retention
            ),
            "weekly_summary": self._weekly_summary(
                channel_key, channel_stats, growth, retention
            ),
        }

    def _suno_recommendations(
        self, genre_analysis: Dict, retention: Dict
    ) -> Dict[str, Any]:
        top = genre_analysis.get("top_genre", "lofi")
        bottom = genre_analysis.get("bottom_genre", "")
        quality = retention.get("quality_tier", "good")

        prompts_bias = []
        if quality in ("excellent", "good"):
            prompts_bias.append(f"Continue emphasizing {top} style")
            prompts_bias.append("Maintain current tempo and energy levels")
        else:
            prompts_bias.append(f"Shift toward {top} (top performer)")
            prompts_bias.append("Try stronger melodic hooks in first 30 seconds")

        if bottom and bottom != top:
            prompts_bias.append(f"Reduce frequency of {bottom} (underperforming)")

        return {
            "preferred_genre": top,
            "avoid_genre": bottom if bottom != top else None,
            "prompt_bias": prompts_bias,
            "energy_level": "maintain" if quality in ("excellent", "good") else "increase",
        }

    def _thumbnail_recommendations(
        self, genre_analysis: Dict, growth: Dict
    ) -> Dict[str, Any]:
        trajectory = growth.get("trajectory", "steady")
        top = genre_analysis.get("top_genre", "lofi")

        if trajectory == "accelerating":
            style = "Keep current thumbnail style -- it's working"
        elif trajectory == "steady":
            style = "Try bolder colors or contrasting text overlays"
        else:
            style = "Redesign thumbnails with brighter palette and clearer text"

        return {
            "style_recommendation": style,
            "color_palette": self._genre_colors(top),
            "text_overlay": trajectory != "accelerating",
        }

    @staticmethod
    def _genre_colors(genre: str) -> List[str]:
        palettes = {
            "lofi": ["#2D1B69", "#FF6B6B", "#4ECDC4"],
            "ambient": ["#0D1B2A", "#1B263B", "#415A77"],
            "jazz": ["#D4A574", "#8B6914", "#2C1810"],
            "classical": ["#F5F5DC", "#C0C0C0", "#333333"],
            "rain": ["#4A6FA5", "#166088", "#2E4057"],
            "piano": ["#1A1A2E", "#E94560", "#16213E"],
        }
        for key, colors in palettes.items():
            if key in genre.lower():
                return colors
        return ["#1DB954", "#191414", "#FFFFFF"]

    def _timing_recommendations(self, growth: Dict) -> Dict[str, Any]:
        trajectory = growth.get("trajectory", "steady")
        return {
            "recommended_upload_time": "06:00 UTC (early morning peak)",
            "frequency": (
                "daily" if trajectory == "accelerating"
                else "daily" if trajectory == "steady"
                else "every other day (focus on quality)"
            ),
            "best_days": ["Monday", "Wednesday", "Friday", "Sunday"],
        }

    def _focus_recommendations(
        self, genre_analysis: Dict, growth: Dict, retention: Dict
    ) -> Dict[str, Any]:
        trajectory = growth.get("trajectory", "steady")
        quality = retention.get("quality_tier", "good")
        top = genre_analysis.get("top_genre", "lofi")

        if trajectory == "accelerating" and quality in ("excellent", "good"):
            priority = "Scale what's working -- increase upload frequency"
            action = f"Double down on {top} content"
        elif trajectory == "steady":
            priority = "Optimize for growth -- experiment with adjacent genres"
            action = f"Keep {top} as anchor, test 1-2 new styles weekly"
        else:
            priority = "Rebuild engagement -- focus on retention"
            action = "Shorter tracks, stronger hooks, thumbnail A/B testing"

        return {
            "priority": priority,
            "action": action,
            "top_genre_focus": top,
        }

    def _weekly_summary(
        self,
        channel_key: str,
        stats: Dict,
        growth: Dict,
        retention: Dict,
    ) -> Dict[str, str]:
        trajectory = growth.get("trajectory", "steady")
        subs = stats.get("subscribers", 0)
        net = growth.get("weekly_growth_rate", 0)
        quality = retention.get("quality_tier", "good")

        return {
            "channel": channel_key,
            "subscribers": subs,
            "weekly_growth": f"+{net}/week",
            "trajectory": trajectory,
            "retention_quality": quality,
            "headline": (
                f"{channel_key}: {trajectory} growth, {quality} retention, {subs} subs"
            ),
        }


# ===========================================================================
# FirestoreWriter
# ===========================================================================
class FirestoreWriter:
    """Writes analytics reports to Firestore analytics_reports collection."""

    def __init__(self):
        self.db = None
        self._init_firestore()

    def _init_firestore(self):
        """Initialize Firestore client from environment."""
        try:
            import firebase_admin
            from firebase_admin import credentials, firestore

            service_account = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "")
            if not service_account:
                logger.warning("FIREBASE_SERVICE_ACCOUNT not set.")
                return

            cred_dict = json.loads(service_account)
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_dict)
                firebase_admin.initialize_app(cred)

            self.db = firestore.client()
            logger.info("Firestore client initialized for analytics writer.")
        except Exception as e:
            logger.warning(f"Firestore init failed: {e}")

    def write_report(self, channel_key: str, report: Dict[str, Any]) -> bool:
        """Write a channel analytics report to Firestore."""
        if not self.db:
            logger.warning("Firestore unavailable -- skipping report write.")
            return False
        try:
            from firebase_admin import firestore as fs

            doc_id = f"{channel_key}_{datetime.utcnow().strftime('%Y%m%d')}"
            doc_ref = self.db.collection("analytics_reports").document(doc_id)
            doc_ref.set(
                {
                    **report,
                    "timestamp": fs.SERVER_TIMESTAMP,
                }
            )
            logger.info(f"Analytics report written: {doc_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to write analytics report: {e}")
            return False

    def update_latest(self, all_reports: Dict[str, Any]) -> bool:
        """Update the analytics_reports/latest document with all channel data."""
        if not self.db:
            logger.warning("Firestore unavailable -- skipping latest update.")
            return False
        try:
            from firebase_admin import firestore as fs

            doc_ref = self.db.collection("analytics_reports").document("latest")
            doc_ref.set(
                {
                    "channels": all_reports,
                    "updated_at": fs.SERVER_TIMESTAMP,
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                }
            )
            logger.info("Updated analytics_reports/latest.")
            return True
        except Exception as e:
            logger.error(f"Failed to update latest report: {e}")
            return False


# ===========================================================================
# Main entry point
# ===========================================================================
def main() -> Dict[str, Any]:
    """
    Run the full analytics pipeline:
    1. Fetch YouTube data for all channels
    2. Analyze patterns against channel_identity.yaml
    3. Generate recommendations
    4. Write to Firestore and recommendations.yaml
    """
    logger.info("=" * 60)
    logger.info("  AI Analytics Agent -- Starting Analysis")
    logger.info("=" * 60)

    fetcher = YouTubeDataFetcher()
    analyzer = PatternAnalyzer()
    engine = RecommendationEngine()
    writer = FirestoreWriter()

    all_reports: Dict[str, Any] = {}

    for display_key, channel_id in CHANNEL_MAP.items():
        # Convert hyphen key to underscore for identity lookup
        identity_key = display_key.replace("-", "_")
        logger.info(f"\n--- Analyzing: {display_key} ({channel_id}) ---")

        try:
            # 1. Fetch data
            stats = fetcher.fetch_channel_stats(channel_id)
            videos = fetcher.fetch_recent_videos(channel_id)
            analytics = fetcher.fetch_analytics(channel_id)

            # 2. Analyze patterns
            genre_analysis = analyzer.analyze_genre_performance(
                identity_key, videos, analytics
            )
            growth = analyzer.analyze_growth_velocity(analytics)
            retention = analyzer.analyze_retention_patterns(analytics)

            # 3. Generate recommendations
            recommendations = engine.generate_recommendations(
                display_key, stats, genre_analysis, growth, retention
            )

            all_reports[display_key] = {
                "stats": stats,
                "genre_analysis": genre_analysis,
                "growth": growth,
                "retention": retention,
                "recommendations": recommendations,
            }

            # 4. Write individual report to Firestore
            writer.write_report(display_key, all_reports[display_key])

            logger.info(
                f"  -> {display_key}: {growth.get('trajectory', '?')} growth, "
                f"{retention.get('quality_tier', '?')} retention"
            )

        except Exception as e:
            logger.error(f"Analysis failed for {display_key}: {e}", exc_info=True)
            all_reports[display_key] = {"error": str(e)}

    # 5. Update latest combined report in Firestore
    writer.update_latest(all_reports)

    # 6. Write recommendations.yaml for pipeline to read
    try:
        recs_output = {}
        for ch_key, data in all_reports.items():
            if "recommendations" in data:
                recs_output[ch_key] = data["recommendations"]

        with open("recommendations.yaml", "w") as f:
            yaml.dump(
                {"recommendations": recs_output, "generated_at": datetime.utcnow().isoformat() + "Z"},
                f,
                default_flow_style=False,
                sort_keys=False,
            )
        logger.info("Wrote recommendations.yaml")
    except Exception as e:
        logger.error(f"Failed to write recommendations.yaml: {e}")

    logger.info("=" * 60)
    logger.info("  AI Analytics Agent -- Analysis Complete")
    logger.info("=" * 60)

    return all_reports


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    main()

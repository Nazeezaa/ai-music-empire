"""
pipeline_health.py - Health check and error tracking for AI Music Empire pipeline

Tracks pass/fail status of each pipeline step, maps common errors to human-readable
fix suggestions, and writes run summaries to Firestore "pipeline_runs" collection.
"""

import time
import logging
import re
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Common error -> fix mappings
# ---------------------------------------------------------------------------
ERROR_FIX_MAP = [
    {
        "pattern": r"503",
        "fix": "Suno API is temporarily down. Will retry next scheduled run.",
    },
    {
        "pattern": r"40[13]",
        "fix": (
            "Suno cookie expired. Update SUNO_COOKIE in GitHub Secrets "
            "with a fresh __client cookie from suno.com"
        ),
    },
    {
        "pattern": r"(?i)youtube.*upload|upload.*youtube|HttpError",
        "fix": (
            "Check YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, "
            "YOUTUBE_REFRESH_TOKEN in GitHub Secrets"
        ),
    },
    {
        "pattern": r"(?i)firebase|firestore|service.account",
        "fix": "Check FIREBASE_SERVICE_ACCOUNT secret",
    },
    {
        "pattern": r"(?i)ffmpeg|subprocess|concat|audio",
        "fix": "FFmpeg binary issue -- check GitHub Actions runner",
    },
]


def _match_fix(error_message: str, step: str) -> str:
    """Return the best suggested fix for a given error message, or a generic one."""
    for entry in ERROR_FIX_MAP:
        if re.search(entry["pattern"], error_message):
            return entry["fix"]

    # Fallback fix suggestions per step
    step_fallbacks = {
        "suno_auth": (
            "Suno cookie may be expired. Update SUNO_COOKIE in GitHub Secrets."
        ),
        "suno_generate": (
            "Music generation failed. Check Suno API status and SUNO_COOKIE."
        ),
        "ffmpeg_concat": (
            "Audio concatenation failed. Verify FFmpeg is installed on the runner."
        ),
        "youtube_upload": (
            "YouTube upload failed. Verify YouTube OAuth secrets in GitHub Secrets."
        ),
        "firestore_sync": (
            "Firestore operation failed. Check FIREBASE_SERVICE_ACCOUNT secret."
        ),
        "youtube_analytics": (
            "YouTube Analytics sync failed. Check YouTube API credentials."
        ),
    }
    return step_fallbacks.get(step, f"Step '{step}' failed. Check logs for details.")


class HealthCheck:
    """
    Tracks the pass/fail status of every pipeline step and writes a
    summary document to the Firestore ``pipeline_runs`` collection.
    """

    STEPS = {
        "suno_auth": "Suno API authentication",
        "suno_generate": "Music generation",
        "ffmpeg_concat": "Audio concatenation",
        "youtube_upload": "YouTube upload",
        "firestore_sync": "Firestore sync",
        "youtube_analytics": "YouTube Analytics sync",
    }

    def __init__(self, channel: str):
        self.channel = channel
        self._start_time = time.time()
        self._checklist: Dict[str, Dict[str, str]] = {}

        # Pre-populate every step as "pending"
        for step_key, description in self.STEPS.items():
            self._checklist[step_key] = {
                "status": "pending",
                "message": description,
                "fix": "",
            }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_pass(self, step: str, message: Optional[str] = None) -> None:
        """Record a step as passed."""
        desc = self.STEPS.get(step, step)
        self._checklist[step] = {
            "status": "pass",
            "message": message or f"{desc} succeeded",
            "fix": "",
        }
        logger.info(f"[HealthCheck] PASS  {step}: {self._checklist[step]['message']}")

    def check_fail(
        self,
        step: str,
        error: Exception,
        fix: Optional[str] = None,
    ) -> None:
        """Record a step as failed with an error and suggested fix."""
        error_msg = str(error)
        suggested_fix = fix or _match_fix(error_msg, step)
        desc = self.STEPS.get(step, step)
        self._checklist[step] = {
            "status": "fail",
            "message": f"{desc} failed: {error_msg}",
            "fix": suggested_fix,
        }
        logger.error(
            f"[HealthCheck] FAIL  {step}: {error_msg} | Fix: {suggested_fix}"
        )

    @property
    def overall_status(self) -> str:
        """
        Derive an overall status from individual step results.

        Returns:
            "success"  - all steps passed
            "partial"  - some steps passed, some failed or were skipped
            "failed"   - no steps passed (or critical early step failed)
        """
        statuses = [v["status"] for v in self._checklist.values()]
        passed = statuses.count("pass")
        failed = statuses.count("fail")

        if failed == 0 and passed == len(statuses):
            return "success"
        if passed > 0:
            return "partial"
        return "failed"

    @property
    def duration_seconds(self) -> float:
        return round(time.time() - self._start_time, 2)

    def summary_dict(self) -> Dict[str, Any]:
        """Build the summary document that will be stored in Firestore."""
        return {
            "checklist": self._checklist,
            "overall_status": self.overall_status,
            "channel": self.channel,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "duration_seconds": self.duration_seconds,
        }

    # ------------------------------------------------------------------
    # Firestore persistence
    # ------------------------------------------------------------------

    def save_to_firestore(self, db) -> bool:
        """
        Write the health-check summary to the ``pipeline_runs`` collection.

        Args:
            db: A Firestore client instance (may be ``None`` if Firestore
                is unavailable).

        Returns:
            True on success, False otherwise.
        """
        if db is None:
            logger.warning(
                "[HealthCheck] Firestore client is None -- skipping save."
            )
            self._log_summary_to_console()
            return False

        try:
            from firebase_admin import firestore as fs

            doc = self.summary_dict()
            # Use server timestamp when writing to Firestore
            doc["timestamp"] = fs.SERVER_TIMESTAMP

            db.collection("pipeline_runs").add(doc)
            logger.info(
                f"[HealthCheck] Saved health report to Firestore "
                f"(status={self.overall_status}, duration={self.duration_seconds}s)"
            )
            return True
        except Exception as e:
            logger.error(f"[HealthCheck] Failed to save to Firestore: {e}")
            self._log_summary_to_console()
            return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _log_summary_to_console(self) -> None:
        """Pretty-print the health report to the logger as a fallback."""
        logger.info("=" * 60)
        logger.info(f"  Pipeline Health Report  |  Channel: {self.channel}")
        logger.info(f"  Overall: {self.overall_status}  |  Duration: {self.duration_seconds}s")
        logger.info("-" * 60)
        for step, info in self._checklist.items():
            icon = "PASS" if info["status"] == "pass" else "FAIL"
            logger.info(f"  [{icon}] {step}: {info['message']}")
            if info["fix"]:
                logger.info(f"         Fix: {info['fix']}")
        logger.info("=" * 60)

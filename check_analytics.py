"""
check_analytics.py - Check YouTube Analytics for uploaded videos
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
import requests
import yaml

logger = logging.getLogger(__name__)


def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def get_access_token(config):
    from upload_youtube import get_access_token as _get_token
    return _get_token(config)


def get_channel_analytics(config, days=7):
    access_token = get_access_token(config)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    url = "https://youtubeanalytics.googleapis.com/v2/reports"
    params = {"ids": "channel==MINE", "startDate": start_date, "endDate": end_date, "metrics": "views,estimatedMinutesWatched,averageViewDuration,subscribersGained,likes", "dimensions": "day", "sort": "day"}
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        rows = data.get("rows", [])
        total_views = sum(r[1] for r in rows)
        total_subs = sum(r[4] for r in rows)
        summary = {"period": f"{start_date} to {end_date}", "total_views": total_views, "total_subscribers_gained": total_subs}
        logger.info(f"Analytics ({days}d): {total_views} views, {total_subs} new subs")
        report_path = os.path.join(config["pipeline"]["output_dir"], f"analytics_{end_date}.json")
        os.makedirs(config["pipeline"]["output_dir"], exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(summary, f, indent=2)
        return summary
    except requests.exceptions.RequestException as e:
        logger.error(f"Analytics failed: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = load_config()
    get_channel_analytics(config)

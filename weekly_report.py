#!/usr/bin/env python3
"""
Weekly Report Generator for AI Music Empire
- Markdown weekly report
- Revenue progress tracking
- Channel comparison
- AI-generated recommendations
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

LEARNING_HISTORY_PATH = "data/learning_history.json"
REVENUE_PATH = "data/revenue.json"
REPORT_OUTPUT_DIR = "reports"


def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _parse_ts(ts):
    try:
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return datetime.min


class ChannelStats:
    """Compute weekly stats for a single channel."""

    def __init__(self, channel_name, history):
        self.channel_name = channel_name
        self.entries = [e for e in history if e.get("channel") == channel_name]

    def weekly_entries(self, weeks_back=1):
        cutoff = datetime.utcnow() - timedelta(weeks=weeks_back)
        return [e for e in self.entries if _parse_ts(e.get("timestamp", "")) >= cutoff]

    def avg_score(self, weeks_back=1):
        entries = self.weekly_entries(weeks_back)
        scores = [e.get("score", 0) for e in entries]
        return round(sum(scores) / len(scores), 2) if scores else 0

    def total_videos(self, weeks_back=1):
        return len(self.weekly_entries(weeks_back))

    def top_genre(self, weeks_back=1):
        entries = self.weekly_entries(weeks_back)
        genre_scores = {}
        for e in entries:
            g = e.get("genre", "unknown")
            genre_scores.setdefault(g, []).append(e.get("score", 0))
        if not genre_scores:
            return "N/A"
        return max(genre_scores, key=lambda g: sum(genre_scores[g]) / len(genre_scores[g]))

    def score_trend(self):
        this_week = self.avg_score(1)
        last_week = self.avg_score(2)
        if last_week == 0:
            return "New"
        diff = this_week - last_week
        if diff > 2:
            return "Up +" + str(round(diff, 1))
        elif diff < -2:
            return "Down " + str(round(diff, 1))
        return "Stable"


class RevenueTracker:
    """Track revenue progress toward goals."""

    def __init__(self, revenue_data):
        self.data = revenue_data

    def monthly_total(self):
        channels = self.data.get("channels", {})
        return sum(ch.get("monthly_revenue", 0) for ch in channels.values())

    def monthly_goal(self):
        return self.data.get("monthly_goal", 1000)

    def progress_percent(self):
        goal = self.monthly_goal()
        return round(self.monthly_total() / goal * 100, 1) if goal else 0

    def channel_breakdown(self):
        channels = self.data.get("channels", {})
        breakdown = []
        for name, info in channels.items():
            breakdown.append({"channel": name, "revenue": info.get("monthly_revenue", 0), "source": info.get("primary_source", "ads"), "trend": info.get("trend", "stable")})
        breakdown.sort(key=lambda x: x["revenue"], reverse=True)
        return breakdown

    def projected_annual(self):
        return round(self.monthly_total() * 12, 2)


class RecommendationEngine:
    """Generate AI recommendations based on weekly data."""

    def generate(self, channel_stats, revenue):
        recommendations = []
        progress = revenue.progress_percent()
        if progress < 50:
            recommendations.append("Revenue at " + str(progress) + "% of monthly goal. Consider increasing upload frequency.")
        elif progress >= 90:
            recommendations.append("Revenue at " + str(progress) + "% of goal! Consider raising next month target by 20%.")
        for cs in channel_stats:
            avg = cs.avg_score(1)
            videos = cs.total_videos(1)
            top_genre = cs.top_genre(1)
            if avg < 40:
                recommendations.append(cs.channel_name + ": Low avg score (" + str(avg) + "). Focus on " + top_genre + ".")
            if videos < 3:
                recommendations.append(cs.channel_name + ": Only " + str(videos) + " videos this week. Aim for 5-7.")
            if avg > 70:
                recommendations.append(cs.channel_name + ": Strong (" + str(avg) + " avg)! Double down on " + top_genre + ".")
        if not recommendations:
            recommendations.append("All channels performing well. Stay the course!")
        return recommendations


def generate_weekly_report(channel_names=None):
    """Generate a Markdown weekly report."""
    history = _load_json(LEARNING_HISTORY_PATH)
    history = history if isinstance(history, list) else []
    revenue_data = _load_json(REVENUE_PATH)
    revenue = RevenueTracker(revenue_data)
    if not channel_names:
        channel_names = list({e.get("channel", "Unknown") for e in history})
    channel_stats = [ChannelStats(name, history) for name in channel_names]
    rec_engine = RecommendationEngine()
    recommendations = rec_engine.generate(channel_stats, revenue)
    now = datetime.utcnow()
    week_start = (now - timedelta(days=now.weekday())).strftime("%B %d")
    week_end = now.strftime("%B %d, %Y")
    lines = [
        "# AI Music Empire - Weekly Report",
        "**Week of " + week_start + " - " + week_end + "**",
        "*Generated: " + now.strftime("%Y-%m-%d %H:%M UTC") + "*",
        "", "---", "",
        "## Revenue Progress", "",
        "| Metric | Value |",
        "|--------|-------|",
        "| Monthly Revenue | $" + str(round(revenue.monthly_total(), 2)) + " |",
        "| Monthly Goal | $" + str(round(revenue.monthly_goal(), 2)) + " |",
        "| Progress | " + str(revenue.progress_percent()) + "% |",
        "| Projected Annual | $" + str(revenue.projected_annual()) + " |",
        "",
    ]
    breakdown = revenue.channel_breakdown()
    if breakdown:
        lines.extend(["### Revenue by Channel", "", "| Channel | Revenue | Source | Trend |", "|---------|---------|--------|-------|"])
        for ch in breakdown:
            lines.append("| " + ch["channel"] + " | $" + str(ch["revenue"]) + " | " + ch["source"] + " | " + ch["trend"] + " |")
        lines.append("")
    lines.extend(["---", "", "## Channel Performance", "", "| Channel | Videos | Avg Score | Top Genre | Trend |", "|---------|--------|-----------|-----------|-------|"])
    for cs in channel_stats:
        lines.append("| " + cs.channel_name + " | " + str(cs.total_videos(1)) + " | " + str(cs.avg_score(1)) + " | " + cs.top_genre(1) + " | " + cs.score_trend() + " |")
    lines.append("")
    lines.extend(["---", "", "## AI Recommendations", ""])
    for i, rec in enumerate(recommendations, 1):
        lines.append(str(i) + ". " + rec)
    lines.extend(["", "---", "", "*Report generated by AI Music Empire Smart Agent v1.0*"])
    report = "\n".join(lines)
    os.makedirs(REPORT_OUTPUT_DIR, exist_ok=True)
    filename = "weekly_report_" + now.strftime("%Y%m%d") + ".md"
    filepath = os.path.join(REPORT_OUTPUT_DIR, filename)
    with open(filepath, "w") as f:
        f.write(report)
    print("Weekly report saved to " + filepath)
    return report


if __name__ == "__main__":
    report = generate_weekly_report()
    print(report)

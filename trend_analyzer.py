#!/usr/bin/env python3
"""
YouTube Trend Analyzer for AI Music Empire
- YouTube trend analysis & keyword extraction
- Competitor analysis
- Prompt recommendations for Suno/Udio
"""

import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

TREND_CACHE_PATH = "data/trend_cache.json"
COMPETITOR_CACHE_PATH = "data/competitor_cache.json"


class KeywordExtractor:
    """Extract trending keywords from video titles, descriptions, and tags."""

    STOP_WORDS = {
        "the", "a", "an", "is", "it", "to", "in", "for", "of", "and",
        "on", "with", "this", "that", "from", "by", "at", "or", "as",
        "be", "was", "are", "been", "has", "have", "had", "do", "does",
        "will", "would", "could", "should", "may", "might", "can",
        "no", "not", "but", "if", "so", "my", "your", "music", "video",
        "official", "lyrics", "audio", "hour", "hours", "mix", "playlist",
    }

    def extract(self, texts, top_n=30):
        words = []
        for text in texts:
            tokens = re.findall(r"[a-zA-Z]{3,}", text.lower())
            words.extend(t for t in tokens if t not in self.STOP_WORDS)
        counts = Counter(words)
        return [{"keyword": kw, "count": c} for kw, c in counts.most_common(top_n)]

    def extract_bigrams(self, texts, top_n=15):
        bigrams = []
        for text in texts:
            tokens = [t for t in re.findall(r"[a-zA-Z]{3,}", text.lower()) if t not in self.STOP_WORDS]
            bigrams.extend(f"{tokens[i]} {tokens[i+1]}" for i in range(len(tokens) - 1))
        counts = Counter(bigrams)
        return [{"bigram": bg, "count": c} for bg, c in counts.most_common(top_n)]


class TrendAnalyzer:
    """Analyze YouTube trends for AI-generated music niches."""

    NICHES = [
        "lofi hip hop", "ambient music", "synthwave", "chillhop",
        "study music", "relaxing music", "sleep music", "focus music",
        "meditation music", "jazz lofi", "classical piano",
        "nature sounds", "rain sounds", "cafe music",
    ]

    def __init__(self):
        self.extractor = KeywordExtractor()

    def analyze_niche_trends(self, niche, video_data):
        titles = [v.get("title", "") for v in video_data]
        descriptions = [v.get("description", "")[:200] for v in video_data]
        all_tags = []
        for v in video_data:
            all_tags.extend(v.get("tags", []))
        keywords = self.extractor.extract(titles + all_tags)
        bigrams = self.extractor.extract_bigrams(titles + descriptions)
        avg_views = _safe_avg([v.get("views", 0) for v in video_data])
        avg_likes = _safe_avg([v.get("likes", 0) for v in video_data])
        avg_duration = _safe_avg([v.get("duration_minutes", 0) for v in video_data])
        return {
            "niche": niche, "sample_size": len(video_data),
            "top_keywords": keywords[:10], "top_bigrams": bigrams[:5],
            "avg_views": round(avg_views), "avg_likes": round(avg_likes),
            "avg_duration_minutes": round(avg_duration, 1),
            "analyzed_at": datetime.utcnow().isoformat(),
        }

    def get_rising_topics(self, recent, older):
        recent_kw = {k["keyword"]: k["count"] for k in self.extractor.extract([v.get("title","") for v in recent])}
        older_kw = {k["keyword"]: k["count"] for k in self.extractor.extract([v.get("title","") for v in older])}
        rising = []
        for kw, count in recent_kw.items():
            old_count = older_kw.get(kw, 0)
            growth = (count - old_count) / old_count if old_count > 0 else 1.0
            if growth > 0.2:
                rising.append({"keyword": kw, "growth": round(growth * 100, 1), "current_count": count})
        rising.sort(key=lambda x: x["growth"], reverse=True)
        return rising[:15]


class CompetitorAnalyzer:
    """Track and analyze competitor channels."""

    def __init__(self, competitors=None):
        self.competitors = competitors or []

    def analyze(self):
        cache = _load_json(COMPETITOR_CACHE_PATH)
        results = []
        for comp in self.competitors:
            cid = comp.get("channel_id", "")
            name = comp.get("name", cid)
            data = cache.get(cid, {})
            results.append({
                "name": name, "channel_id": cid,
                "subscribers": data.get("subscribers", 0),
                "recent_videos": data.get("recent_video_count", 0),
                "avg_views_last_10": data.get("avg_views_last_10", 0),
                "upload_frequency_days": data.get("upload_frequency_days", 7),
                "top_tags": data.get("top_tags", []),
                "strengths": self._identify_strengths(data),
            })
        return results

    @staticmethod
    def _identify_strengths(data):
        strengths = []
        if data.get("avg_views_last_10", 0) > 50000: strengths.append("high_viewership")
        if data.get("upload_frequency_days", 99) <= 2: strengths.append("frequent_uploads")
        if data.get("subscriber_growth_monthly", 0) > 5: strengths.append("fast_growing")
        if data.get("avg_watch_time_minutes", 0) > 30: strengths.append("high_retention")
        return strengths or ["average_performer"]


class PromptRecommender:
    """Generate Suno/Udio prompt recommendations based on trends."""

    GENRE_TEMPLATES = {
        "lofi": "lofi hip hop, jazzy chords, vinyl crackle, {keywords}, chill vibes, {bpm} bpm",
        "ambient": "ambient soundscape, ethereal pads, {keywords}, atmospheric, dreamy",
        "synthwave": "synthwave, retro 80s, {keywords}, punchy bass, neon, driving rhythm",
        "chillhop": "chillhop, boom bap drums, {keywords}, mellow, Rhodes piano",
        "classical": "classical piano, {keywords}, emotional, orchestral, dynamic",
        "sleep": "sleep music, {keywords}, gentle, soft, calming, very slow tempo",
        "study": "study music, {keywords}, focus, minimal, steady rhythm, no lyrics",
    }

    def recommend(self, trend_data, genre="lofi"):
        keywords = [k["keyword"] for k in trend_data.get("top_keywords", [])[:5]]
        keyword_str = ", ".join(keywords) if keywords else "relaxing, chill"
        template = self.GENRE_TEMPLATES.get(genre, self.GENRE_TEMPLATES["lofi"])
        prompts = [template.format(keywords=keyword_str, bpm="75")]
        bigrams = [b["bigram"] for b in trend_data.get("top_bigrams", [])[:3]]
        if bigrams:
            prompts.append(template.format(keywords=", ".join(bigrams), bpm="80"))
        month = datetime.utcnow().month
        if month in (11, 12, 1, 2):
            prompts.append(f"cozy winter {genre}, warm tones, fireplace ambience, {keyword_str}")
        elif month in (6, 7, 8):
            prompts.append(f"summer {genre}, bright, breezy, {keyword_str}, uplifting")
        else:
            prompts.append(f"peaceful {genre}, {keyword_str}, smooth transitions")
        return prompts

    def recommend_titles(self, trend_data, genre="lofi"):
        keywords = [k["keyword"] for k in trend_data.get("top_keywords", [])[:3]]
        kw = " ".join(w.title() for w in keywords) if keywords else "Peaceful Vibes"
        return [
            f"{kw} - {genre.title()} Mix for Study & Relaxation",
            f"1 Hour {genre.title()} | {kw}",
            f"{genre.title()} Beats - {kw} [Chill Mix 2026]",
            f"{kw} | {genre.title()} Music to Focus",
            f"Best {genre.title()} Playlist | {kw} Edition",
        ]


def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _safe_avg(values):
    return sum(values) / len(values) if values else 0.0


def run_trend_analysis():
    print(f"Trend Analyzer running at {datetime.utcnow().isoformat()}")
    analyzer = TrendAnalyzer()
    recommender = PromptRecommender()
    cache = _load_json(TREND_CACHE_PATH)
    results = {}
    for niche in TrendAnalyzer.NICHES[:5]:
        niche_key = niche.replace(" ", "_")
        video_data = cache.get(niche_key, [])
        if video_data:
            trend = analyzer.analyze_niche_trends(niche, video_data)
            prompts = recommender.recommend(trend, genre=niche.split()[0])
            titles = recommender.recommend_titles(trend, genre=niche.split()[0])
            results[niche] = {"trend": trend, "recommended_prompts": prompts, "recommended_titles": titles}
            print(f"  {niche}: keywords={[k['keyword'] for k in trend['top_keywords'][:5]]}")
    os.makedirs("data", exist_ok=True)
    with open("data/trend_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("Trend analysis complete.")
    return results


if __name__ == "__main__":
    run_trend_analysis()

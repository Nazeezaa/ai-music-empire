#!/usr/bin/env python3
"""
Smart AI Agent for AI Music Empire
"""

import json
import os
from datetime import datetime
from pathlib import Path

CONFIG_PATH = os.environ.get("AGENT_CONFIG", "agent_config.yaml")
LEARNING_HISTORY_PATH = "data/learning_history.json"
ANALYTICS_CACHE_PATH = "data/analytics_cache.json"


class YouTubeAnalytics:
    def __init__(self, channel_id, api_key=None):
        self.channel_id = channel_id
        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY", "")

    def get_recent_videos(self, days=30):
        cache = _load_json(ANALYTICS_CACHE_PATH)
        return cache.get(self.channel_id, {}).get("recent_videos", [])

    def get_video_performance(self, video_id):
        cache = _load_json(ANALYTICS_CACHE_PATH)
        return cache.get(self.channel_id, {}).get("video_performance", {}).get(video_id, {})

    def get_channel_stats(self):
        cache = _load_json(ANALYTICS_CACHE_PATH)
        return cache.get(self.channel_id, {}).get("channel_stats", {})


class TrackScorer:
    DEFAULT_WEIGHTS = {"views": 0.25, "watch_time_hours": 0.20, "ctr_percent": 0.20, "retention_percent": 0.15, "likes_ratio": 0.10, "comments": 0.10}

    def __init__(self, weights=None):
        self.weights = weights or self.DEFAULT_WEIGHTS

    def score(self, metrics):
        n = self._normalize(metrics)
        return round(min(max(sum(n.get(k,0)*w for k,w in self.weights.items()),0),100),2)

    @staticmethod
    def _normalize(m):
        return {"views": min(m.get("views",0)/10000*100,100), "watch_time_hours": min(m.get("watch_time_hours",0)/500*100,100), "ctr_percent": min(m.get("ctr_percent",0)/15*100,100), "retention_percent": m.get("retention_percent",0), "likes_ratio": min(m.get("likes_ratio",0)/10*100,100), "comments": min(m.get("comments",0)/200*100,100)}


class GenrePromptAdjuster:
    def __init__(self, learning_rate=0.1):
        self.learning_rate = learning_rate

    def suggest_adjustments(self, history):
        if not history:
            return {"action": "no_change", "reason": "insufficient data"}
        gs = {}
        for e in history[-50:]:
            gs.setdefault(e.get("genre","unknown"),[]).append(e.get("score",0))
        avg = {g: sum(s)/len(s) for g,s in gs.items()}
        return {"increase_weight": max(avg,key=avg.get), "decrease_weight": min(avg,key=avg.get), "avg_scores": avg}


class UploadTimeOptimizer:
    def __init__(self):
        self.hour_scores = {h: [] for h in range(24)}

    def feed(self, history):
        for e in history:
            h = e.get("upload_hour")
            if h is not None:
                self.hour_scores[h].append(e.get("score",0))

    def best_hours(self, top_n=3):
        avgs = [{"hour":h,"avg_score":round(sum(s)/len(s),2),"samples":len(s)} for h,s in self.hour_scores.items() if s]
        avgs.sort(key=lambda x: x["avg_score"], reverse=True)
        return avgs[:top_n]


def load_learning_history():
    return _load_json(LEARNING_HISTORY_PATH) if os.path.exists(LEARNING_HISTORY_PATH) else []

def save_learning_history(history):
    os.makedirs(os.path.dirname(LEARNING_HISTORY_PATH), exist_ok=True)
    with open(LEARNING_HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2, default=str)

def append_learning_entry(entry):
    history = load_learning_history()
    entry["timestamp"] = datetime.utcnow().isoformat()
    history.append(entry)
    save_learning_history(history)

def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def load_config():
    try:
        import yaml
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception:
        return {"scoring_weights": TrackScorer.DEFAULT_WEIGHTS, "learning_rate": 0.1, "channels": []}

def run_agent():
    print(f"Smart Agent running at {datetime.utcnow().isoformat()}")
    config = load_config()
    scorer = TrackScorer(weights=config.get("scoring_weights"))
    adjuster = GenrePromptAdjuster(learning_rate=config.get("learning_rate", 0.1))
    optimizer = UploadTimeOptimizer()
    history = load_learning_history()
    optimizer.feed(history)
    for ch in config.get("channels", []):
        cid, cname = ch.get("channel_id",""), ch.get("name","")
        print(f"Processing channel: {cname}")
        yt = YouTubeAnalytics(cid)
        for video in yt.get_recent_videos(days=7):
            perf = yt.get_video_performance(video.get("video_id",""))
            score = scorer.score(perf)
            print(f"  Track: {video.get('title','Unknown')} -> score: {score}")
            append_learning_entry({"channel": cname, "video_id": video.get("video_id"), "title": video.get("title"), "genre": video.get("genre","unknown"), "score": score, "upload_hour": video.get("upload_hour"), "metrics": perf})
    history = load_learning_history()
    print("Suggestions:", json.dumps(adjuster.suggest_adjustments(history), indent=2))
    print("Best times:", optimizer.best_hours())
    print("Agent cycle complete.")

if __name__ == "__main__":
    run_agent()

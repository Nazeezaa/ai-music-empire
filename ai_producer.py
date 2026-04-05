"""
AI Music Producer Agent
=======================
Intelligent music producer that automatically decides what music to create
for each channel based on analytics data. Replaces random prompt selection
with data-driven creative decisions.

Usage:
    from ai_producer import AIProducer
    producer = AIProducer()
    session = producer.produce_session("lofi-barista")
"""

import os
import json
import random
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import yaml

# Firebase imports (graceful fallback)
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHANNEL_MAP = {
    "lofi-barista": "UCC76tzAG4JH8YYtehJAhwCg",
    "rain-walker": "UCTS92ipxcFAe3DIL4BYmycQ",
    "velvet-groove": "UCqDKZ3d6A3baCnmVp5JahJw",
    "piano-ghost": "UCEbUj2bHA6kzm_GKrEB-YTw",
}

CHANNEL_SLUGS = list(CHANNEL_MAP.keys())

# Underscore variants used in channel_identity.yaml
SLUG_TO_KEY = {
    "lofi-barista": "lofi_barista",
    "rain-walker": "rain_walker",
    "velvet-groove": "velvet_groove",
    "piano-ghost": "piano_ghost",
}

# Trending element pools per channel niche (used as creative spice)
TRENDING_ELEMENTS = {
    "lofi-barista": [
        "vinyl crackle", "tape hiss", "rain on window background",
        "jazz piano chords", "muted trumpet melody", "bossa nova guitar",
        "coffee shop ambiance", "typewriter clicks", "soft Rhodes keys",
        "lo-fi dusty drums", "warm analog bass",
    ],
    "rain-walker": [
        "thunderstorm ambiance", "gentle rain on leaves", "city rain sounds",
        "distant thunder rolls", "rain on tin roof", "ocean waves blend",
        "foghorn in distance", "wind chimes in rain", "puddle footsteps",
        "dripping water echoes", "forest rain atmosphere",
    ],
    "velvet-groove": [
        "wah-wah guitar", "slap bass groove", "smooth saxophone solo",
        "vintage Rhodes electric piano", "funk clavinet", "70s disco strings",
        "retro synth pad", "jazzy trumpet licks", "bongo percussion",
        "soul vocal chops", "Hammond organ swells",
    ],
    "piano-ghost": [
        "reverb-drenched piano", "haunting celeste melody", "music box tinkle",
        "ghostly choir pad", "distant church bells", "creaking floorboards",
        "whispering wind textures", "melancholic violin", "glass harmonica",
        "detuned piano notes", "ethereal harp arpeggios",
    ],
}

# BPM defaults per channel (fallback when no analytics available)
DEFAULT_BPM = {
    "lofi-barista": (70, 90),
    "rain-walker": (0, 60),
    "velvet-groove": (80, 120),
    "piano-ghost": (60, 100),
}

# Track count range per session
DEFAULT_TRACK_COUNT = (3, 5)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_yaml(path: str) -> Optional[Dict]:
    """Load a YAML file, return None on failure."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"[AIProducer] Warning: Could not load {path}: {e}")
        return None


def _init_firestore():
    """Initialize Firestore client (singleton-safe)."""
    if not FIREBASE_AVAILABLE:
        return None
    try:
        sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "")
        if not sa_json:
            return None
        if not firebase_admin._apps:
            sa_dict = json.loads(sa_json)
            cred = credentials.Certificate(sa_dict)
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"[AIProducer] Warning: Firestore init failed: {e}")
        return None


def _prompt_fingerprint(prompt: str) -> str:
    """Create a short hash of a prompt for dedup checks."""
    return hashlib.md5(prompt.lower().strip().encode()).hexdigest()[:10]


# ---------------------------------------------------------------------------
# Core Producer
# ---------------------------------------------------------------------------

class AIProducer:
    """
    Intelligent music producer that uses analytics to decide what to create.
    Falls back to channel_identity.yaml defaults when analytics are unavailable.
    """

    def __init__(
        self,
        channel_identity_path: str = "channel_identity.yaml",
        recommendations_path: str = "recommendations.yaml",
    ):
        self.channel_identity = _load_yaml(channel_identity_path) or {}
        self.recommendations = self._load_recommendations(recommendations_path)
        self.db = _init_firestore()
        self._recent_prompts: Dict[str, List[str]] = {}  # channel -> list of fingerprints
        self._load_recent_prompts()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_recommendations(self, path: str) -> Dict:
        """Load recommendations from YAML, then overlay Firestore latest."""
        recs = _load_yaml(path) or {}
        recs = recs.get("recommendations", recs)

        # Try Firestore for fresher data
        if self.db:
            try:
                doc = self.db.collection("analytics_reports").document("latest").get()
                if doc.exists:
                    fs_data = doc.to_dict()
                    # Merge Firestore data (it has per-channel recommendations)
                    for slug in CHANNEL_SLUGS:
                        if slug in fs_data and "recommendations" in fs_data[slug]:
                            recs[slug] = {
                                **recs.get(slug, {}),
                                **fs_data[slug]["recommendations"],
                            }
                    print("[AIProducer] Loaded fresh analytics from Firestore.")
            except Exception as e:
                print(f"[AIProducer] Firestore read skipped: {e}")
        return recs

    def _load_recent_prompts(self):
        """Load recent prompt fingerprints from Firestore pipeline_runs."""
        if not self.db:
            return
        try:
            cutoff = datetime.utcnow() - timedelta(days=14)
            runs = (
                self.db.collection("pipeline_runs")
                .where("timestamp", ">=", cutoff)
                .order_by("timestamp", direction=firestore.Query.DESCENDING)
                .limit(50)
                .stream()
            )
            for run in runs:
                data = run.to_dict()
                ch = data.get("channel", "")
                prompt = data.get("suno_prompt", "")
                if ch and prompt:
                    self._recent_prompts.setdefault(ch, []).append(
                        _prompt_fingerprint(prompt)
                    )
        except Exception as e:
            print(f"[AIProducer] Could not load recent prompts: {e}")

    def _get_channel_config(self, channel_slug: str) -> Dict:
        """Get channel config from channel_identity.yaml."""
        key = SLUG_TO_KEY.get(channel_slug, channel_slug.replace("-", "_"))
        channels = self.channel_identity.get("channels", self.channel_identity)
        return channels.get(key, {})

    def _get_channel_recs(self, channel_slug: str) -> Dict:
        """Get recommendations for a channel."""
        return self.recommendations.get(channel_slug, {})

    # ------------------------------------------------------------------
    # Smart prompt generation
    # ------------------------------------------------------------------

    def _pick_best_genres(self, channel_slug: str, config: Dict, recs: Dict) -> List[str]:
        """Pick genres biased toward what analytics say works."""
        all_genres = config.get("sub_genres", [])
        if not all_genres:
            all_genres = config.get("suno_prompts", [])

        tuning = recs.get("suno_prompt_tuning", {})
        preferred = tuning.get("preferred_genre", "").lower()
        avoid = tuning.get("avoid_genre", "").lower()

        # Score each genre
        scored = []
        for genre in all_genres:
            score = 1.0
            gl = genre.lower()
            if preferred and preferred in gl:
                score += 2.0
            if avoid and avoid in gl:
                score -= 1.5
            # Boost genres mentioned in prompt_bias
            for bias_word in tuning.get("prompt_bias", []):
                if bias_word.lower() in gl:
                    score += 0.5
            scored.append((genre, max(score, 0.1)))

        # Weighted random selection (pick top 3)
        scored.sort(key=lambda x: x[1], reverse=True)
        top_pool = scored[: max(len(scored) // 2, 3)]

        weights = [s[1] for s in top_pool]
        total = sum(weights)
        weights = [w / total for w in weights]

        chosen = []
        for _ in range(min(3, len(top_pool))):
            pick = random.choices(top_pool, weights=weights, k=1)[0]
            chosen.append(pick[0])

        return chosen

    def _pick_moods(self, config: Dict, recs: Dict) -> List[str]:
        """Pick moods, biased by energy level recommendation."""
        moods = config.get("moods", ["chill", "relaxing", "ambient"])
        energy = recs.get("suno_prompt_tuning", {}).get("energy_level", "maintain")

        # Energy-based mood weighting
        high_energy = ["energetic", "upbeat", "groovy", "lively", "bright"]
        low_energy = ["peaceful", "dreamy", "melancholic", "gentle", "soft"]

        scored = []
        for mood in moods:
            score = 1.0
            ml = mood.lower()
            if energy == "increase":
                if any(h in ml for h in high_energy):
                    score += 1.0
            elif energy == "decrease":
                if any(l in ml for l in low_energy):
                    score += 1.0
            scored.append((mood, score))

        weights = [s[1] for s in scored]
        picked = random.choices(scored, weights=weights, k=min(2, len(scored)))
        return [p[0] for p in picked]

    def _build_suno_prompt(self, channel_slug: str, config: Dict, recs: Dict) -> str:
        """Build a creative, varied Suno prompt based on analytics."""
        # Start with base prompts from channel identity
        base_prompts = config.get("suno_prompts", [])
        genres = self._pick_best_genres(channel_slug, config, recs)
        moods = self._pick_moods(config, recs)
        instruments = config.get("instruments", [])
        trending = TRENDING_ELEMENTS.get(channel_slug, [])

        # Strategy 1: Enhance a base prompt with trending elements
        # Strategy 2: Build a fully custom prompt from components
        strategy = random.choice(["enhance", "custom", "hybrid"])

        if strategy == "enhance" and base_prompts:
            base = random.choice(base_prompts)
            # Add 1-2 trending elements
            spice = random.sample(trending, min(2, len(trending)))
            prompt = f"{base}, {', '.join(spice)}"

        elif strategy == "custom":
            # Build from scratch
            parts = []
            if genres:
                parts.append(random.choice(genres))
            if moods:
                parts.append(random.choice(moods))
            if instruments:
                parts.extend(random.sample(instruments, min(2, len(instruments))))
            if trending:
                parts.append(random.choice(trending))
            prompt = ", ".join(parts)

        else:  # hybrid
            if base_prompts:
                base = random.choice(base_prompts)
                parts = base.split(",")
                # Keep first half, replace second half with fresh elements
                keep = parts[: len(parts) // 2]
                fresh = []
                if genres:
                    fresh.append(random.choice(genres))
                if trending:
                    fresh.append(random.choice(trending))
                if moods:
                    fresh.append(random.choice(moods))
                prompt = ", ".join([p.strip() for p in keep + fresh])
            else:
                prompt = ", ".join(genres + moods)

        # Dedup check: if this prompt was recently used, mutate it
        fp = _prompt_fingerprint(prompt)
        recent = self._recent_prompts.get(channel_slug, [])
        attempts = 0
        while fp in recent and attempts < 5:
            # Mutate by swapping a trending element
            if trending:
                swap = random.choice(trending)
                prompt = prompt + f", {swap}"
            fp = _prompt_fingerprint(prompt)
            attempts += 1

        return prompt.strip().rstrip(",").strip()

    # ------------------------------------------------------------------
    # Title & metadata generation
    # ------------------------------------------------------------------

    def _get_next_volume(self, channel_slug: str) -> int:
        """Get next volume number from Firestore or estimate from date."""
        if self.db:
            try:
                uploads = (
                    self.db.collection("uploads")
                    .where("channel", "==", channel_slug)
                    .order_by("uploaded_at", direction=firestore.Query.DESCENDING)
                    .limit(1)
                    .stream()
                )
                for doc in uploads:
                    data = doc.to_dict()
                    title = data.get("title", "")
                    # Try to extract volume number from title
                    import re
                    vol_match = re.search(r"Vol\.?\s*(\d+)", title)
                    if vol_match:
                        return int(vol_match.group(1)) + 1
            except Exception:
                pass

        # Fallback: estimate from day of year
        day_of_year = datetime.utcnow().timetuple().tm_yday
        return day_of_year % 100 + 1

    def _generate_title(self, channel_slug: str, config: Dict, mood: str) -> str:
        """Generate an SEO-optimized YouTube title."""
        templates = config.get("youtube_title_templates", [])
        volume = self._get_next_volume(channel_slug)

        if templates:
            template = random.choice(templates)
            # Replace template variables
            title = template.replace("{volume}", str(volume))
            title = title.replace("{mood}", mood if mood else "Chill")
            title = title.replace("{duration}", "1 Hour")
            title = title.replace("{date}", datetime.utcnow().strftime("%B %Y"))
            title = title.replace("{genre}", config.get("sub_genres", [""])[0] if config.get("sub_genres") else "")
            return title

        # Fallback title generation
        channel_names = {
            "lofi-barista": "Lofi Barista",
            "rain-walker": "Rain Walker",
            "velvet-groove": "Velvet Groove",
            "piano-ghost": "Piano Ghost",
        }
        name = channel_names.get(channel_slug, channel_slug.replace("-", " ").title())
        return f"{name} Vol. {volume} | {mood} | 1 Hour Mix"

    def _generate_description(
        self, channel_slug: str, config: Dict, prompt: str, mood: str, track_count: int
    ) -> str:
        """Generate a compelling YouTube description with timestamps."""
        channel_names = {
            "lofi-barista": "Lofi Barista",
            "rain-walker": "Rain Walker",
            "velvet-groove": "Velvet Groove",
            "piano-ghost": "Piano Ghost",
        }
        name = channel_names.get(channel_slug, channel_slug.replace("-", " ").title())

        # Build description
        lines = []
        lines.append(f"{name} presents a fresh {mood.lower()} session.")
        lines.append(f"Perfect for studying, relaxing, working, or winding down.")
        lines.append("")

        # Timestamps (estimated ~2-3 min per track)
        lines.append("Timestamps:")
        time_cursor = 0
        for i in range(track_count):
            minutes = time_cursor // 60
            seconds = time_cursor % 60
            lines.append(f"  {minutes:02d}:{seconds:02d} - Track {i + 1}")
            time_cursor += random.randint(120, 200)  # 2-3.3 min per track

        lines.append("")
        lines.append(f"Style: {prompt[:120]}")
        lines.append("")

        # Tags as hashtags
        tags = config.get("tags", [])
        if tags:
            hashtags = " ".join(f"#{t.replace(' ', '').replace('-', '')}" for t in tags[:8])
            lines.append(hashtags)

        lines.append("")
        lines.append(f"Subscribe to {name} for daily {mood.lower()} music!")
        lines.append("Produced with AI Music Empire")

        return "\n".join(lines)

    def _generate_tags(self, config: Dict, mood: str, genres: List[str]) -> List[str]:
        """Generate relevant YouTube tags."""
        base_tags = config.get("tags", [])
        extra = [mood, "1 hour", "study music", "relaxing", "ambient", "chill"]
        extra.extend(genres[:3])
        combined = list(set(base_tags + extra))
        return combined[:30]  # YouTube allows max ~500 chars / ~30 tags

    # ------------------------------------------------------------------
    # Thumbnail style selection
    # ------------------------------------------------------------------

    def _select_thumbnail_style(self, channel_slug: str, config: Dict, recs: Dict) -> Dict:
        """Recommend thumbnail style based on CTR data and channel config."""
        artwork = config.get("artwork_style", {})
        thumb_recs = recs.get("thumbnail_style", {})

        # Base style from channel identity
        style = {
            "color_palette": artwork.get("color_palette", ["#1a1a2e", "#16213e", "#0f3460"]),
            "visual_elements": artwork.get("visual_elements", []),
            "font_style": artwork.get("font_style", "modern"),
        }

        # Apply analytics-based adjustments
        rec_text = thumb_recs.get("recommendation", "").lower()
        if "vibrant" in rec_text or "bright" in rec_text:
            style["brightness"] = "high"
            style["saturation"] = "boosted"
        elif "dark" in rec_text or "moody" in rec_text:
            style["brightness"] = "low"
            style["saturation"] = "muted"

        if "text" in rec_text and "large" in rec_text:
            style["text_size"] = "large"

        # Color palette from recommendation if available
        if "color_palette" in thumb_recs:
            style["color_palette"] = thumb_recs["color_palette"]

        return style

    # ------------------------------------------------------------------
    # Production planning
    # ------------------------------------------------------------------

    def _plan_bpm_range(self, channel_slug: str, recs: Dict) -> tuple:
        """Select BPM range based on retention data."""
        defaults = DEFAULT_BPM.get(channel_slug, (70, 100))

        tuning = recs.get("suno_prompt_tuning", {})
        energy = tuning.get("energy_level", "maintain")

        low, high = defaults
        if energy == "increase":
            low = min(low + 10, high)
            high = min(high + 15, 180)
        elif energy == "decrease":
            low = max(low - 10, 0)
            high = max(high - 10, low + 10)

        return (low, high)

    def _plan_track_count(self, recs: Dict) -> int:
        """Decide track count per session."""
        # Default 3-5 tracks
        low, high = DEFAULT_TRACK_COUNT
        focus = recs.get("channel_focus", {})
        priority = focus.get("priority", "medium")

        if priority == "high":
            return random.randint(4, 5)  # More tracks for high-priority channels
        elif priority == "low":
            return random.randint(2, 3)
        return random.randint(low, high)

    def _plan_track_order(self, track_count: int, bpm_range: tuple) -> List[Dict]:
        """Plan track order for the final video (energy arc)."""
        low, high = bpm_range
        tracks = []

        for i in range(track_count):
            # Energy arc: start mellow, build, then wind down
            position = i / max(track_count - 1, 1)
            if position < 0.3:
                # Opening: gentle
                bpm = random.randint(low, low + (high - low) // 3)
                energy = "gentle"
            elif position < 0.7:
                # Middle: peak energy
                bpm = random.randint(low + (high - low) // 3, high)
                energy = "peak"
            else:
                # Closing: wind down
                bpm = random.randint(low, low + (high - low) // 2)
                energy = "wind_down"

            tracks.append({
                "position": i + 1,
                "bpm": bpm,
                "energy": energy,
            })

        return tracks

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def produce_session(self, channel_slug: str) -> Dict[str, Any]:
        """
        Produce a complete music session for a channel.

        Returns a dict with all production parameters:
            - suno_prompt: str
            - title: str
            - description: str
            - tags: list[str]
            - thumbnail_style: dict
            - track_count: int
            - bpm_range: tuple
            - track_plan: list[dict]
            - channel_slug: str
            - channel_id: str
            - volume_number: int
            - mood: str
            - genres: list[str]
        """
        print(f"\n{'='*60}")
        print(f"[AIProducer] Producing session for: {channel_slug}")
        print(f"{'='*60}")

        config = self._get_channel_config(channel_slug)
        recs = self._get_channel_recs(channel_slug)

        if not config:
            print(f"[AIProducer] Warning: No config for {channel_slug}, using defaults.")

        # 1. Smart prompt generation
        suno_prompt = self._build_suno_prompt(channel_slug, config, recs)
        genres = self._pick_best_genres(channel_slug, config, recs)
        moods = self._pick_moods(config, recs)
        mood = moods[0] if moods else "chill"

        # 2. Title & metadata
        volume = self._get_next_volume(channel_slug)
        title = self._generate_title(channel_slug, config, mood)
        tags = self._generate_tags(config, mood, genres)

        # 3. Production planning
        bpm_range = self._plan_bpm_range(channel_slug, recs)
        track_count = self._plan_track_count(recs)
        track_plan = self._plan_track_order(track_count, bpm_range)

        # 4. Description (needs track_count)
        description = self._generate_description(
            channel_slug, config, suno_prompt, mood, track_count
        )

        # 5. Thumbnail style
        thumbnail_style = self._select_thumbnail_style(channel_slug, config, recs)

        session = {
            "suno_prompt": suno_prompt,
            "title": title,
            "description": description,
            "tags": tags,
            "thumbnail_style": thumbnail_style,
            "track_count": track_count,
            "bpm_range": bpm_range,
            "track_plan": track_plan,
            "channel_slug": channel_slug,
            "channel_id": CHANNEL_MAP.get(channel_slug, ""),
            "volume_number": volume,
            "mood": mood,
            "genres": genres,
        }

        # Log the session
        self._log_session(session)

        print(f"\n[AIProducer] Session ready:")
        print(f"  Prompt:  {suno_prompt[:80]}...")
        print(f"  Title:   {title}")
        print(f"  Tracks:  {track_count} (BPM {bpm_range[0]}-{bpm_range[1]})")
        print(f"  Mood:    {mood}")
        print(f"  Genres:  {', '.join(genres[:3])}")
        print(f"  Volume:  {volume}")

        return session

    def _log_session(self, session: Dict):
        """Log production session to Firestore for dedup and analytics."""
        if not self.db:
            return
        try:
            self.db.collection("producer_sessions").add({
                "channel": session["channel_slug"],
                "suno_prompt": session["suno_prompt"],
                "title": session["title"],
                "mood": session["mood"],
                "genres": session["genres"],
                "track_count": session["track_count"],
                "bpm_range": list(session["bpm_range"]),
                "volume_number": session["volume_number"],
                "prompt_fingerprint": _prompt_fingerprint(session["suno_prompt"]),
                "created_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            print(f"[AIProducer] Could not log session: {e}")


# ---------------------------------------------------------------------------
# Convenience function for run_pipeline.py integration
# ---------------------------------------------------------------------------

_producer_instance: Optional[AIProducer] = None


def get_producer() -> AIProducer:
    """Get or create the singleton producer instance."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = AIProducer()
    return _producer_instance


def produce_session(channel_slug: str) -> Dict[str, Any]:
    """
    Convenience function for pipeline integration.

    Usage in run_pipeline.py:
        from ai_producer import produce_session
        session = produce_session("lofi-barista")
        suno_prompt = session["suno_prompt"]
        title = session["title"]
    """
    return get_producer().produce_session(channel_slug)


# ---------------------------------------------------------------------------
# CLI for testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    channel = sys.argv[1] if len(sys.argv) > 1 else "lofi-barista"

    if channel == "all":
        producer = AIProducer()
        for slug in CHANNEL_SLUGS:
            session = producer.produce_session(slug)
            print(f"\n{'~'*60}\n")
    else:
        if channel not in CHANNEL_MAP:
            print(f"Unknown channel: {channel}")
            print(f"Available: {', '.join(CHANNEL_SLUGS)}")
            sys.exit(1)
        session = produce_session(channel)

    print("\nDone.")

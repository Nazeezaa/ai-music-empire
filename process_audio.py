"""
process_audio.py - Process and normalize audio using FFmpeg
"""
import os
import subprocess
import json
import logging
import yaml

logger = logging.getLogger(__name__)


def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def check_ffmpeg():
    try:
        result = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            logger.info(f"FFmpeg found")
            return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    logger.error("FFmpeg not found")
    return False


def process_track(input_path, config=None):
    if config is None:
        config = load_config()
    if not check_ffmpeg():
        return None
    if not os.path.exists(input_path):
        logger.error(f"Input not found: {input_path}")
        return None
    ac = config["audio_processing"]
    target_lufs = ac["normalize_loudness"]
    fade_in = ac["fade_in_seconds"]
    fade_out = ac["fade_out_seconds"]
    sample_rate = ac["sample_rate"]
    bitrate = ac["bitrate"]
    base, ext = os.path.splitext(input_path)
    output_path = f"{base}_processed{ext}"
    duration = get_duration(input_path)
    if duration is None:
        logger.error("Could not determine duration")
        return None
    fade_out_start = max(0, duration - fade_out)
    filters = [f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11"]
    if fade_in > 0:
        filters.append(f"afade=t=in:st=0:d={fade_in}")
    if fade_out > 0:
        filters.append(f"afade=t=out:st={fade_out_start}:d={fade_out}")
    cmd = ["ffmpeg", "-y", "-i", input_path, "-af", ",".join(filters), "-ar", str(sample_rate), "-b:a", bitrate, "-codec:a", "libmp3lame", output_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            logger.info(f"Processed: {output_path} ({os.path.getsize(output_path)/1024/1024:.1f} MB)")
            # Create video for YouTube upload
            video_path = audio_to_video(output_path)
            return video_path or output_path
        else:
            logger.error(f"FFmpeg error: {result.stderr[-500:]}")
            return None
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timed out")
        return None


def audio_to_video(audio_path):
    """Convert audio to mp4 video with solid color background for YouTube upload."""
    base = os.path.splitext(audio_path)[0]
    video_path = f"{base}.mp4"
    duration = get_duration(audio_path)
    if duration is None:
        duration = 300  # fallback 5 min
    # Create video with dark purple background + audio
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"color=c=0x1a0a2e:s=1920x1080:d={duration}:r=1",
        "-i", audio_path,
        "-c:v", "libx264", "-tune", "stillimage", "-preset", "ultrafast",
        "-c:a", "aac", "-b:a", "192k",
        "-shortest", "-pix_fmt", "yuv420p",
        video_path
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            logger.info(f"Video created: {video_path} ({os.path.getsize(video_path)/1024/1024:.1f} MB)")
            return video_path
        else:
            logger.error(f"Video creation failed: {result.stderr[-500:]}")
            return None
    except subprocess.TimeoutExpired:
        logger.error("Video creation timed out")
        return None


def get_duration(input_path):
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", input_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return float(result.stdout.strip())
    except Exception as e:
        logger.warning(f"Could not get duration: {e}")
    return None


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python process_audio.py <input_file>")
        sys.exit(1)
    config = load_config()
    result = process_track(sys.argv[1], config)
    if result:
        print(f"SUCCESS: {result}")
    else:
        print("FAILED")
        sys.exit(1)

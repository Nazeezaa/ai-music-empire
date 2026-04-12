"""
process_audio.py - Process, concatenate, and normalize audio using FFmpeg
"""
import os
import subprocess
import json
import logging
import tempfile
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


def concatenate_audio(audio_files, output_path=None, config=None):
    """Concatenate multiple audio files into a single file using FFmpeg concat demuxer.

    Args:
        audio_files: List of audio file paths to concatenate.
        output_path: Output file path. If None, auto-generates in output_dir.
        config: Pipeline config dict. Loaded from config.yaml if None.

    Returns:
        Path to concatenated audio file, or None on failure.
    """
    if config is None:
        config = load_config()
    if not check_ffmpeg():
        return None
    if not audio_files:
        logger.error("No audio files to concatenate")
        return None
    if len(audio_files) == 1:
        logger.info("Only one audio file, skipping concatenation")
        return audio_files[0]

    # Verify all input files exist
    for f in audio_files:
        if not os.path.exists(f):
            logger.error(f"Input file not found: {f}")
            return None

    if output_path is None:
        output_dir = config["pipeline"]["output_dir"]
        os.makedirs(output_dir, exist_ok=True)
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f"concatenated_{ts}.mp3")

    # Create concat list file for FFmpeg demuxer
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, dir=config["pipeline"]["output_dir"]) as list_file:
            for audio_file in audio_files:
                # FFmpeg concat demuxer requires absolute paths with proper escaping
                abs_path = os.path.abspath(audio_file)
                escaped = abs_path.replace("'", "'\\''")
                list_file.write(f"file '{escaped}'\n")
            list_path = list_file.name
        logger.info(f"Concatenating {len(audio_files)} files into {output_path}")

        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", list_path,
            "-c", "copy",
            output_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            size_mb = os.path.getsize(output_path) / 1024 / 1024
            duration = get_duration(output_path)
            logger.info(f"Concatenated: {output_path} ({size_mb:.1f} MB, {duration:.0f}s)")
            return output_path
        else:
            logger.error(f"Concatenation failed: {result.stderr[-500:]}")
            return None
    except subprocess.TimeoutExpired:
        logger.error("Concatenation timed out")
        return None
    except Exception as e:
        logger.error(f"Concatenation error: {e}")
        return None
    finally:
        # Clean up the temp concat list file
        try:
            os.unlink(list_path)
        except Exception:
            pass


def loop_to_duration(input_path, target_duration, config=None):
    """Loop/repeat audio to fill a target duration using FFmpeg .

    If the input audio is already >= target_duration, it is trimmed to exactly
    target_duration. If shorter, it is looped until the target is reached.

    Args:
        input_path: Path to input audio file.
        target_duration: Target duration in seconds (e.g. 3600 for 1 hour).
        config: Pipeline config dict. Loaded from config.yaml if None.

    Returns:
        Path to the looped audio file, or input_path if looping is not needed
        or fails.
    """
    if config is None:
        config = load_config()
    if not check_ffmpeg():
        return input_path
    if not os.path.exists(input_path):
        logger.error(f"Input not found for looping: {input_path}")
        return input_path

    current_duration = get_duration(input_path)
    if current_duration is None:
        logger.warning("Could not determine duration for looping, skipping")
        return input_path

    # If already at or very close to target, no looping needed
    if current_duration >= target_duration - 5:
        logger.info(f"Audio already {current_duration:.0f}s >= target {target_duration}s, trimming if needed")
        if current_duration <= target_duration + 5:
            return input_path
        # Trim to exact target
        base, ext = os.path.splitext(input_path)
        trimmed_path = f"{base}_trimmed{ext}"
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-t", str(target_duration),
            "-c", "copy",
            trimmed_path
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                logger.info(f"Trimmed to {target_duration}s: {trimmed_path}")
                return trimmed_path
        except Exception as e:
            logger.warning(f"Trim failed, using original: {e}")
        return input_path

    # Loop the audio to fill target_duration
    base, ext = os.path.splitext(input_path)
    looped_path = f"{base}_looped_{target_duration}s{ext}"

    logger.info(
        f"Looping audio from {current_duration:.0f}s to {target_duration}s "
        f"({target_duration / current_duration:.1f}x repeats)"
    )

    # Use concat demuxer (stream_loop fails on MP3)
    abs_input = os.path.abspath(input_path)
    repeats = int(target_duration / current_duration) + 2
    list_file = f"{base}_loop_list.txt"
    with open(list_file, 'w') as lf:
        for _ in range(repeats):
            lf.write(f"file '{abs_input}'\n")

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file,
        "-t", str(int(target_duration)),
        "-c", "copy",
        looped_path
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            final_duration = get_duration(looped_path)
            size_mb = os.path.getsize(looped_path) / 1024 / 1024
            logger.info(
                f"Looped audio ready: {looped_path} "
                f"({final_duration:.0f}s, {size_mb:.1f} MB)"
            )
            return looped_path
        else:
            logger.error(f"Loop failed: {result.stderr[-500:]}")
            return input_path
    except subprocess.TimeoutExpired:
        logger.error("Loop timed out")
        return input_path
    except Exception as e:
        logger.error(f"Loop error: {e}")
        return input_path
    finally:
        if os.path.exists(list_file):
            os.remove(list_file)


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
        print("Usage: python process_audio.py <input_file> [input_file2 ...]")
        print("  Single file: processes and converts to video")
        print("  Multiple files: concatenates first, then processes")
        sys.exit(1)
    config = load_config()
    if len(sys.argv) > 2:
        # Multiple files: concatenate first
        audio_files = sys.argv[1:]
        concatenated = concatenate_audio(audio_files, config=config)
        if concatenated:
            result = process_track(concatenated, config)
        else:
            print("FAILED: Concatenation failed")
            sys.exit(1)
    else:
        result = process_track(sys.argv[1], config)
    if result:
        print(f"SUCCESS: {result}")
    else:
        print("FAILED")
        sys.exit(1)

"""
generate_thumbnail.py - Auto-generate YouTube thumbnails for AI Music Empire
Uses Pillow to create channel-specific 1280x720 thumbnails with bold colors,
channel branding, volume info, and music-themed geometric elements.
"""

import os
import math
import yaml
import logging
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger("thumbnail")

# -- Channel color palettes (from channel_identity.yaml artwork_style) ------
CHANNEL_PALETTES = {
    "lofi_barista": {
        "bg": "#3B2F2F",
        "accent": "#D4A574",
        "text": "#FFF8F0",
        "shapes": "#8B6F47",
    },
    "rain_walker": {
        "bg": "#1A2332",
        "accent": "#4A90D9",
        "text": "#E0ECF8",
        "shapes": "#2C5F8A",
    },
    "velvet_groove": {
        "bg": "#2D1B33",
        "accent": "#D4AF37",
        "text": "#F5E6CC",
        "shapes": "#8B3A62",
    },
    "piano_drifter": {
        "bg": "#1C1C2E",
        "accent": "#C0C0C0",
        "text": "#EAEAEA",
        "shapes": "#4A4A6A",
    },
}

CHANNEL_DISPLAY_NAMES = {
    "lofi_barista": "LOFI BARISTA",
    "rain_walker": "RAIN WALKER",
    "velvet_groove": "VELVET GROOVE",
    "piano_drifter": "PIANO DRIFTER",
}

WIDTH, HEIGHT = 1280, 720


def _hex_to_rgb(hex_color):
    """Convert hex color string to RGB tuple."""
    hex_color = hex_color.lstrip("#")
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def _draw_music_note(draw, x, y, size, color):
    """Draw a simple music note symbol."""
    # Note head (filled ellipse)
    draw.ellipse(
        [x, y, x + size, y + size * 0.7],
        fill=color,
    )
    # Note stem
    draw.rectangle(
        [x + size - size * 0.15, y - size * 1.2, x + size, y + size * 0.35],
        fill=color,
    )


def _draw_circle(draw, cx, cy, radius, color, outline_only=False):
    """Draw a circle, filled or outline only."""
    bbox = [cx - radius, cy - radius, cx + radius, cy + radius]
    if outline_only:
        draw.ellipse(bbox, outline=color, width=3)
    else:
        draw.ellipse(bbox, fill=color)


def _draw_geometric_elements(draw, palette, width, height):
    """Add music-themed geometric decorations across the thumbnail."""
    shape_color = _hex_to_rgb(palette["shapes"])
    accent_color = _hex_to_rgb(palette["accent"])

    # Music notes scattered around
    _draw_music_note(draw, 80, 100, 30, shape_color)
    _draw_music_note(draw, width - 150, 80, 25, shape_color)
    _draw_music_note(draw, 120, height - 180, 22, shape_color)

    # Decorative circles
    _draw_circle(draw, width - 100, height - 120, 40, shape_color, outline_only=True)
    _draw_circle(draw, 60, height // 2, 25, accent_color, outline_only=True)
    _draw_circle(draw, width - 80, height // 2 + 60, 15, shape_color)

    # Horizontal accent line
    line_y = height - 220
    draw.rectangle(
        [width // 2 - 250, line_y, width // 2 + 250, line_y + 4],
        fill=accent_color,
    )

    # Small diamond shape top-right
    dx, dy = width - 160, 120
    diamond_size = 18
    draw.polygon(
        [
            (dx, dy - diamond_size),
            (dx + diamond_size, dy),
            (dx, dy + diamond_size),
            (dx - diamond_size, dy),
        ],
        fill=shape_color,
    )


def _load_font(size, bold=True):
    """Try to load a suitable font, fall back to default."""
    font_paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    ]
    if not bold:
        font_paths = [p.replace("-Bold", "") for p in font_paths]

    for path in font_paths:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    # Fallback to default
    try:
        return ImageFont.truetype("arial.ttf", size)
    except OSError:
        return ImageFont.load_default()


def generate_thumbnail(channel_key, volume_number=1, mood="", output_dir="thumbnails"):
    """
    Generate a 1280x720 YouTube thumbnail for the given channel.

    Args:
        channel_key: Channel identifier (e.g. 'lofi_barista')
        volume_number: Volume/episode number
        mood: Mood text to display (e.g. 'chill vibes')
        output_dir: Directory to save the thumbnail

    Returns:
        str: Path to the generated thumbnail PNG
    """
    os.makedirs(output_dir, exist_ok=True)

    # Load palette (fall back to defaults if channel unknown)
    palette = CHANNEL_PALETTES.get(channel_key, CHANNEL_PALETTES["lofi_barista"])
    display_name = CHANNEL_DISPLAY_NAMES.get(channel_key, channel_key.upper())

    # Try to read artwork_style from channel_identity.yaml for overrides
    try:
        with open("channel_identity.yaml", "r") as f:
            identity = yaml.safe_load(f)
        ch_config = identity.get("channels", {}).get(channel_key, {})
        artwork = ch_config.get("artwork_style", {})
        if artwork.get("primary_colors"):
            colors = artwork["primary_colors"]
            if len(colors) >= 2:
                palette["bg"] = colors[0] if colors[0].startswith("#") else palette["bg"]
                palette["accent"] = colors[1] if colors[1].startswith("#") else palette["accent"]
    except Exception as e:
        logger.warning(f"Could not read channel_identity.yaml: {e}")

    # Create image with solid background
    bg_color = _hex_to_rgb(palette["bg"])
    img = Image.new("RGB", (WIDTH, HEIGHT), bg_color)
    draw = ImageDraw.Draw(img)

    # Draw geometric decorations
    _draw_geometric_elements(draw, palette, WIDTH, HEIGHT)

    # -- Channel name (large, bold, centered) --
    title_font = _load_font(90, bold=True)
    text_color = _hex_to_rgb(palette["text"])
    accent_color = _hex_to_rgb(palette["accent"])

    bbox = draw.textbbox((0, 0), display_name, font=title_font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    title_x = (WIDTH - tw) // 2
    title_y = (HEIGHT // 2) - th - 30

    # Drop shadow
    shadow_offset = 4
    draw.text(
        (title_x + shadow_offset, title_y + shadow_offset),
        display_name,
        font=title_font,
        fill=(0, 0, 0),
    )
    draw.text((title_x, title_y), display_name, font=title_font, fill=text_color)

    # -- Volume number --
    vol_font = _load_font(42, bold=True)
    vol_text = f"Vol. {volume_number}"
    vbbox = draw.textbbox((0, 0), vol_text, font=vol_font)
    vw = vbbox[2] - vbbox[0]
    vol_x = (WIDTH - vw) // 2
    vol_y = title_y + th + 30
    draw.text((vol_x, vol_y), vol_text, font=vol_font, fill=accent_color)

    # -- Mood text --
    if mood:
        mood_font = _load_font(32, bold=False)
        mood_display = mood.lower()
        mbbox = draw.textbbox((0, 0), mood_display, font=mood_font)
        mw = mbbox[2] - mbbox[0]
        mood_x = (WIDTH - mw) // 2
        mood_y = vol_y + 60
        draw.text((mood_x, mood_y), mood_display, font=mood_font, fill=text_color)

    # Save thumbnail
    filename = f"{channel_key}_vol{volume_number}_thumbnail.png"
    output_path = os.path.join(output_dir, filename)
    img.save(output_path, "PNG", quality=95)
    logger.info(f"Thumbnail saved: {output_path}")

    return output_path


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    channel = sys.argv[1] if len(sys.argv) > 1 else "lofi_barista"
    vol = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    mood_arg = sys.argv[3] if len(sys.argv) > 3 else "chill vibes"
    path = generate_thumbnail(channel, vol, mood_arg)
    print(f"Generated: {path}")

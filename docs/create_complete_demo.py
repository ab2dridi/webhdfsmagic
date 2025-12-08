#!/usr/bin/env python3
"""
Create a complete animated GIF showing ALL webhdfsmagic features.
Demonstrates: mkdir, put, ls, cat, get, chmod, rm
"""

from PIL import Image, ImageDraw, ImageFont
import os

# Configuration
WIDTH = 1000
HEIGHT = 620
BG_COLOR = '#2e3436'
TEXT_COLOR = '#d3d7cf'
PROMPT_COLOR = '#8ae234'
OUTPUT_COLOR = '#729fcf'
HEADER_COLOR = '#fcaf3e'

def create_frame(text_lines, font_size=13):
    """Create a single frame with text."""
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)
    
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", font_size)
    except:
        font = ImageFont.load_default()
    
    y = 20
    line_height = font_size + 6
    
    for line, color in text_lines:
        draw.text((20, y), line, fill=color, font=font)
        y += line_height
    
    return img

def main():
    frames = []
    durations = []
    
    # Frame 1: Title + Context (2s)
    frames.append(create_frame([
        ("╔════════════════════════════════════════════════════════════╗", HEADER_COLOR),
        ("║         webhdfsmagic - Jupyter Magic for HDFS              ║", HEADER_COLOR),
        ("╚════════════════════════════════════════════════════════════╝", HEADER_COLOR),
        ("", TEXT_COLOR),
        ("Complete demo: mkdir → put → ls → cat → get → chmod → rm", TEXT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(2000)
    
    # Frame 2: mkdir (1.5s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("In [1]: %hdfs mkdir /demo/project", PROMPT_COLOR),
        ("", TEXT_COLOR),
        ("✓ Created directory: /demo/project", OUTPUT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(1500)
    
    # Frame 3: put (1.5s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("In [2]: %hdfs put local_data.csv /demo/project/", PROMPT_COLOR),
        ("", TEXT_COLOR),
        ("✓ Uploaded: /demo/project/local_data.csv (1.2KB)", OUTPUT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(1500)
    
    # Frame 4: ls with DataFrame output (2.5s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("In [3]: df = %hdfs ls /demo/project", PROMPT_COLOR),
        ("        df", PROMPT_COLOR),
        ("", TEXT_COLOR),
        ("✓ Found 1 item", OUTPUT_COLOR),
        ("", TEXT_COLOR),
        ("   path                           type  size   modified", TEXT_COLOR),
        ("0  /demo/project/local_data.csv   file  1.2KB  2024-12-08", TEXT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(2500)
    
    # Frame 5: cat with preview (2.5s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("In [4]: %hdfs cat /demo/project/local_data.csv -n 3", PROMPT_COLOR),
        ("", TEXT_COLOR),
        ("✓ Reading /demo/project/local_data.csv", OUTPUT_COLOR),
        ("", TEXT_COLOR),
        ("   id  name           city          country", TEXT_COLOR),
        ("0  1   Alice Johnson  New York      USA", TEXT_COLOR),
        ("1  2   Bob Smith      London        UK", TEXT_COLOR),
        ("2  3   Carol Davis    Paris         France", TEXT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(2500)
    
    # Frame 6: get (1.5s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("In [5]: %hdfs get /demo/project/local_data.csv downloads/", PROMPT_COLOR),
        ("", TEXT_COLOR),
        ("✓ Downloaded: downloads/local_data.csv (1.2KB)", OUTPUT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(1500)
    
    # Frame 7: chmod (1.5s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("In [6]: %hdfs chmod 755 /demo/project/local_data.csv", PROMPT_COLOR),
        ("", TEXT_COLOR),
        ("✓ Permissions changed: rwxr-xr-x", OUTPUT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(1500)
    
    # Frame 8: rm (1.5s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("In [7]: %hdfs rm -r /demo/project", PROMPT_COLOR),
        ("", TEXT_COLOR),
        ("✓ Deleted: /demo/project", OUTPUT_COLOR),
        ("", TEXT_COLOR),
    ]))
    durations.append(1500)
    
    # Frame 9: Summary with all features (3s)
    frames.append(create_frame([
        ("", TEXT_COLOR),
        ("════════════════════════════════════════════════════════════", HEADER_COLOR),
        ("  Complete HDFS workflow from Jupyter notebooks! 🚀", HEADER_COLOR),
        ("════════════════════════════════════════════════════════════", HEADER_COLOR),
        ("", TEXT_COLOR),
        ("✨ All operations demonstrated:", TEXT_COLOR),
        ("", TEXT_COLOR),
        ("  📁 mkdir   - Create directories on HDFS", TEXT_COLOR),
        ("  ⬆️  put     - Upload local files to HDFS", TEXT_COLOR),
        ("  📋 ls      - List files (returns pandas DataFrame)", TEXT_COLOR),
        ("  👀 cat     - Preview CSV/JSON/text files", TEXT_COLOR),
        ("  ⬇️  get     - Download files from HDFS", TEXT_COLOR),
        ("  🔐 chmod   - Change file permissions", TEXT_COLOR),
        ("  🗑️  rm      - Remove files/directories", TEXT_COLOR),
        ("", TEXT_COLOR),
        ("  + Wildcard support, SSL/Kerberos auth, logging...", TEXT_COLOR),
    ]))
    durations.append(3000)
    
    # Save as GIF
    output_path = "/workspaces/webhdfsmagic/docs/demo.gif"
    frames[0].save(
        output_path,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True
    )
    
    print(f"✅ Created complete demo GIF: {output_path}")
    size_kb = os.path.getsize(output_path) / 1024
    print(f"   Frames: {len(frames)}")
    print(f"   Duration: {sum(durations)/1000:.1f}s")
    print(f"   Size: {size_kb:.1f} KB")

if __name__ == "__main__":
    main()

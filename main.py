import json
import os
import sys
import re
import asyncio
import logging
import subprocess
import math
import shutil
import uuid 
from pathlib import Path
from pyrogram import Client, errors
from pyrogram.enums import ParseMode
from PIL import Image
from tqdm import tqdm
import yt_dlp

# --- Colab Userdata Integration (Fixed Syntax) ---
IS_COLAB = False
TARGET_CHAT_ID_COLAB = None

try:
    from google.colab import userdata
    # সরাসরি এনভায়রনমেন্ট ভেরিয়েবল থেকে ডাটা নেওয়ার চেষ্টা করা হচ্ছে
    # যদি Secrets-এ না থাকে তবে os.environ থেকে নিবে
    API_ID_VAL = userdata.get('API_ID') or os.environ.get('API_ID')
    API_HASH_VAL = userdata.get('API_HASH') or os.environ.get('API_HASH')
    BOT_TOKEN_VAL = userdata.get('BOT_TOKEN') or os.environ.get('BOT_TOKEN')
    TARGET_CHAT_ID_VAL = userdata.get('TARGET_CHAT_ID') or os.environ.get('TARGET_CHAT_ID')
    
    if API_ID_VAL: os.environ["API_ID"] = str(API_ID_VAL)
    if API_HASH_VAL: os.environ["API_HASH"] = str(API_HASH_VAL)
    if BOT_TOKEN_VAL: os.environ["BOT_TOKEN"] = str(BOT_TOKEN_VAL)
    if TARGET_CHAT_ID_VAL: TARGET_CHAT_ID_COLAB = str(TARGET_CHAT_ID_VAL)
    
    IS_COLAB = True
except Exception:
    IS_COLAB = False
# ----------------------------------

# --- External Library Check (Hachoir) ---
try:
    from hachoir.metadata import extractMetadata
    from hachoir.parser import createParser
    HACHOIR_AVAILABLE = True
except ImportError:
    print("⚠️ Warning: Hachoir library is not installed. Metadata extraction will only use FFprobe.")
    HACHOIR_AVAILABLE = False
# --- Hachoir Check End ---

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("pyrogram.methods.advanced.save_file").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Global State Management ---
CONFIG_FILE = "bot_config.json"
CLI_USER_ID = 1000
TMP = Path("temp_files")
TMP.mkdir(exist_ok=True) # Create temporary folder
FIXED_RENAME_PREFIX = "[@TA_HD_Anime] Telegram Channel"

# --- New State Management ---
USER_THUMBS = {} # {user_id: path_to_photo_thumb}
USER_THUMB_TIME = {} # {user_id: timestamp_seconds}
USER_CAPTION_CONFIG = {} # {user_id: {...}}
GLOBAL_CONFIG = {} # Stores the core bot token/API details and TARGET CHAT
USER_LANGUAGE_CONFIG = {} # {user_id: 'official'/'fandub'} 
# -----------------------------

# --- Constants ---
VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.avi', '.webm', '.m4v', '.flv', '.mov')
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.webp')

# --- Utility Functions ---

def parse_range_selection(selection_str: str) -> list[int]:
    """Parses selection string like '1,3-5,8' into [1, 3, 4, 5, 8]."""
    indices = []
    parts = selection_str.split(',')
    for part in parts:
        part = part.strip()
        if not part: continue
        if '-' in part:
            try:
                start_str, end_str = part.split('-')
                start = int(start_str)
                end = int(end_str)
                if start <= end: indices.extend(range(start, end + 1))
            except ValueError: print(f"⚠️ Invalid range format skipped: {part}")
        else:
            try: indices.append(int(part))
            except ValueError: print(f"⚠️ Invalid number skipped: {part}")
    
    seen = set()
    unique_indices = []
    for x in indices:
        if x not in seen:
            unique_indices.append(x)
            seen.add(x)
    return unique_indices

def parse_time(time_str: str) -> int:
    """Converts a time string (e.g., '1m 30s' or '5s') into seconds."""
    total_seconds = 0
    try:
        parts = time_str.split()
        if not parts: return 0
        if len(parts) == 1 and parts[0].isdigit(): return int(parts[0])
        for part in parts:
            if 'h' in part: total_seconds += int(part.replace('h', '')) * 3600
            elif 'm' in part: total_seconds += int(part.replace('m', '')) * 60
            elif 's' in part: total_seconds += int(part.replace('s', ''))
            else:
                if part.isdigit(): total_seconds += int(part)
        return total_seconds
    except Exception: return 0

def parse_size(size_str: str) -> int:
    """Converts a size string (e.g., '100MB', '1.9GB') into bytes."""
    size_str = size_str.upper().replace(" ", "")
    if not size_str: return 0
    match = re.match(r'(\d+\.?\d*)([MGK]B)$', size_str)
    if not match: return 0
    value = float(match.group(1))
    unit = match.group(2)
    if unit == 'KB': return int(value * 1024)
    elif unit == 'MB': return int(value * 1024 * 1024)
    elif unit == 'GB': return int(value * 1024 * 1024 * 1024)
    return 0

# --- yt_dlp Custom Functions ---
def progress_hook(d):
    if d['status'] == 'downloading':
        total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
        downloaded = d.get('downloaded_bytes', 0)
        speed = d.get('speed', 0)
        percent = d.get('_percent_str', ' 0%').strip()
        
        total_mb = total / (1024 * 1024)
        downloaded_mb = downloaded / (1024 * 1024)
        speed_mb = speed / (1024 * 1024) if speed else 0
        
        msg = (f"\r\033[K[TA HD] {downloaded_mb:>5.1f}/{total_mb:<5.1f} MB | "
               f"{percent:>5} | Spd: {speed_mb:>5.2f} MB/s")
        sys.stdout.write(msg)
        sys.stdout.flush()
    
    elif d['status'] == 'finished':
        sys.stdout.write("\n\n[TA HD] 100% Downloaded. Merging files (Please wait)...\n")
        sys.stdout.flush()

def clear_screen():
    os.system('clear' if os.name == 'posix' else 'cls')

def get_quality_format(choice):
    quality_map = {
        '0': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        '1': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best',
        '2': 'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best',
        '3': 'bestvideo[height<=560][ext=mp4]+bestaudio[ext=m4a]/best[height<=560][ext=mp4]/best',
        '4': 'bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best',
        '5': 'bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360][ext=mp4]/best',
        '6': 'bestvideo[height<=240][ext=mp4]+bestaudio[ext=m4a]/best[height<=240][ext=mp4]/best',
        '7': 'bestvideo[height<=144][ext=mp4]+bestaudio[ext=m4a]/best[height<=144][ext=mp4]/best',
        '8': 'bestaudio/best'
    }
    return quality_map.get(choice, 'best')

def save_queue(queue_data):
    with open('queue.json', 'w') as f:
        json.dump(queue_data, f, indent=4)

def load_queue():
    if os.path.exists('queue.json'):
        with open('queue.json', 'r') as f:
            return json.load(f)
    return []
# -------------------------------

# --- ADVANCED INTERACTIVE FILE EXPLORER ---
def interactive_file_explorer(start_path: Path, valid_extensions: tuple, folder_select_mode: bool = False) -> Path | None:
    current_path = start_path
    if not current_path.exists():
        current_path = Path(os.path.abspath(start_path))
        if not current_path.exists():
             print(f"❌ Path does not exist: {start_path}")
             return None

    while True:
        if current_path.is_file():
            if current_path.suffix.lower() in valid_extensions: return current_path
            else: print("❌ Not a valid file."); return None

        try:
            all_items = sorted(list(current_path.iterdir()), key=lambda x: (not x.is_dir(), x.name.lower()))
        except PermissionError:
            print(f"❌ Permission denied: {current_path}")
            current_path = current_path.parent; continue

        display_options = []
        for item in all_items:
            if item.is_dir(): display_options.append(item)
            elif item.is_file() and item.suffix.lower() in valid_extensions: display_options.append(item)

        print(f"\n📂 **Path:** {current_path}")
        print(f" 0 > 🔙 Back")
        if folder_select_mode:
            print(f" s > ✅ Select This Current Folder")

        if not display_options: print("   (Empty or no valid files)")
        
        for idx, item in enumerate(display_options):
            icon = "📁" if item.is_dir() else "🎬" if item.suffix in VIDEO_EXTENSIONS else "🖼️"
            print(f" {idx + 1} > {icon} {item.name}")

        sel = input("\nSelect Number (or 'c' cancel): ").strip().lower()
        if sel == 'c': return None
        if sel == '0': current_path = current_path.parent; continue
        if folder_select_mode and sel == 's': return current_path

        try:
            idx = int(sel) - 1
            if 0 <= idx < len(display_options):
                selected = display_options[idx]
                if selected.is_dir(): current_path = selected 
                else: return selected 
            else: print("❌ Invalid number.")
        except ValueError: print("❌ Invalid input.")
# ------------------------------------------

def get_video_metadata(file_path: Path) -> dict:
    data = {'duration': 0, 'width': 0, 'height': 0}
    if not file_path.exists(): return data
    try:
        cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", str(file_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        video_stream = None
        for stream in metadata.get('streams', []):
            if stream.get('codec_type') == 'video': video_stream = stream; break
        if video_stream:
            data['width'] = int(video_stream.get('width', 0))
            data['height'] = int(video_stream.get('height', 0))
        duration_str = metadata.get('format', {}).get('duration')
        if not duration_str and video_stream: duration_str = video_stream.get('duration')
        if duration_str: data['duration'] = int(float(duration_str))
    except:
        if HACHOIR_AVAILABLE:
            try:
                parser = createParser(str(file_path))
                if parser:
                    with parser:
                        h_metadata = extractMetadata(parser)
                        if h_metadata:
                            if h_metadata.has("duration") and data['duration'] == 0: data['duration'] = int(h_metadata.get("duration").total_seconds())
                            if h_metadata.has("width") and data['width'] == 0: data['width'] = int(h_metadata.get("width"))
                            if h_metadata.has("height") and data['height'] == 0: data['height'] = int(h_metadata.get("height"))
            except: pass
    return data

def get_audio_stream_info(file_path: Path) -> list[dict]:
    streams = []
    try:
        cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", str(file_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        metadata = json.loads(result.stdout)
        audio_streams = [s for s in metadata.get('streams', []) if s.get('codec_type') == 'audio']
        for i, stream in enumerate(audio_streams):
            title = stream.get('tags', {}).get('title', 'N/A')
            language = stream.get('tags', {}).get('language', 'und')
            codec_name = stream.get('codec_name', 'unknown')
            streams.append({
                'relative_index': i,
                'language': language,
                'title': title,
                'codec': codec_name,
                'description': f"Codec: {codec_name.upper()}, Lang: {language.upper()}, Title: {title}"
            })
    except Exception as e: logger.error(f"FFprobe audio error: {e}")
    return streams

def generate_video_thumbnail(video_path: Path, thumb_path: Path, timestamp_sec: int = 1) -> bool:
    try:
        if timestamp_sec < 1: timestamp_sec = 1
        time_format = f"{timestamp_sec // 3600:02d}:{(timestamp_sec % 3600) // 60:02d}:{timestamp_sec % 60:02d}"
        cmd = ["ffmpeg", "-y", "-i", str(video_path), "-ss", time_format, "-vframes", "1", "-vf", "scale=320:-1", str(thumb_path)]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except: return False

def run_ffmpeg_command_with_progress(cmd: list, total_duration_sec: int, description: str):
    TIME_REGEX = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.\d{2}")
    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, universal_newlines=True)
    with tqdm(total=total_duration_sec, unit="s", desc=description, dynamic_ncols=True) as pbar:
        while True:
            line = process.stderr.readline()
            if not line: break
            match = TIME_REGEX.search(line)
            if match:
                h, m, s = map(int, match.groups())
                current_time = h * 3600 + m * 60 + s
                pbar.update(current_time - pbar.n)
        process.wait()
    if process.returncode != 0: raise subprocess.CalledProcessError(process.returncode, cmd, output=process.stderr.read())

async def process_metadata_and_rename(input_path: Path, output_path: Path, duration_sec: int):
    def sync_process():
        audio_info = get_audio_stream_info(input_path)
        cmd = ["ffmpeg", "-y", "-i", str(input_path), "-map", "0", "-c", "copy"]
        if audio_info:
            for i in range(len(audio_info)): cmd.extend([f"-metadata:s:a:{i}", f"title={FIXED_RENAME_PREFIX}"])
        cmd.append(str(output_path))
        run_ffmpeg_command_with_progress(cmd, duration_sec, "Renaming & Metadata Update")
        if not (output_path.exists() and output_path.stat().st_size > 0): raise Exception("FFmpeg failed.")
        return True
    try: return await asyncio.to_thread(sync_process)
    except Exception as e: logger.error(f"Metadata failed: {e}"); raise

async def modify_audio_tracks_and_copy(input_path: Path, output_path: Path, audio_map_indices: list[int], duration_sec: int):
    if not audio_map_indices: raise ValueError("Empty indices.")
    def sync_modify():
        cmd = ["ffmpeg", "-y", "-i", str(input_path), "-c", "copy", "-map", "0:v:0"]
        for i, user_index in enumerate(audio_map_indices):
            if user_index <= 0: continue
            cmd.extend(["-map", f"0:a:{user_index - 1}", f"-metadata:s:a:{i}", f"title={FIXED_RENAME_PREFIX}"])
        cmd.extend(["-map", "0:s?", "-map", "0:d?", "-disposition:a:0", "default", str(output_path)])
        run_ffmpeg_command_with_progress(cmd, duration_sec, f"Audio Modify: {audio_map_indices}")
        if not (output_path.exists() and output_path.stat().st_size > 0): raise Exception("FFmpeg failed.")
        return True
    try: return await asyncio.to_thread(sync_modify)
    except subprocess.CalledProcessError as e: logger.error(f"FFmpeg Audio Failed: {e}"); raise

async def compress_video(input_path: Path, output_path: Path, target_bitrate_kbps: int, duration_sec: int) -> bool:
    AUDIO_BITRATE_KBPS = 128
    video_bitrate_kbps = max(100, target_bitrate_kbps - AUDIO_BITRATE_KBPS)
    def sync_compress():
        cmd_pass1 = ["ffmpeg", "-y", "-i", str(input_path), "-b:v", f"{video_bitrate_kbps}k", "-pass", "1", "-c:v", "libx264", "-preset", "medium", "-an", "-f", "null", "/dev/null"]
        run_ffmpeg_command_with_progress(cmd_pass1, duration_sec, "Pass 1")
        cmd_pass2 = ["ffmpeg", "-y", "-i", str(input_path), "-b:v", f"{video_bitrate_kbps}k", "-pass", "2", "-c:v", "libx264", "-preset", "medium", "-c:a", "aac", "-b:a", f"{AUDIO_BITRATE_KBPS}k", str(output_path)]
        run_ffmpeg_command_with_progress(cmd_pass2, duration_sec, "Pass 2")
        try: os.remove("ffmpeg2pass-0.log"); os.remove("ffmpeg2pass-0.log.mbtree")
        except: pass
        return output_path.exists() and output_path.stat().st_size > 0
    try: return await asyncio.to_thread(sync_compress)
    except: return False

def create_dummy_thumb(path):
    try: Image.new('RGB', (320, 180), 'red').save(path, 'jpeg', quality=85)
    except: pass

def load_config():
    if IS_COLAB:
        print("✅ Using Colab Userdata/Environment configuration.")
        return {
            "bot_token": os.getenv("BOT_TOKEN"),
            "api_id": int(os.getenv("API_ID") or 0),
            "api_hash": os.getenv("API_HASH"),
            "target_chat_id": os.getenv("TARGET_CHAT_ID") or TARGET_CHAT_ID_COLAB
        }

    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config_data = json.load(f)
            print(f"✅ Config loaded: {CONFIG_FILE}")
            global USER_THUMB_TIME, USER_CAPTION_CONFIG, USER_LANGUAGE_CONFIG
            USER_THUMB_TIME.update(config_data.get('user_thumb_time', {}))
            caption_config_raw = config_data.get('user_caption_config', {})
            for k, v in caption_config_raw.items():
                try: USER_CAPTION_CONFIG[int(k)] = v
                except: pass
            language_config_raw = config_data.get('user_language_config', {})
            for k, v in language_config_raw.items():
                try: USER_LANGUAGE_CONFIG[int(k)] = v
                except: pass
            return {
                "bot_token": config_data.get('bot_token', ''),
                "api_id": config_data.get('api_id', 0),
                "api_hash": config_data.get('api_hash', ''),
                "target_chat_id": config_data.get('target_chat_id', '')
            }
    return None

def save_config(core_config_dict):
    full_config = core_config_dict.copy()
    full_config['user_thumb_time'] = {str(k): v for k, v in USER_THUMB_TIME.items()}
    full_config['user_caption_config'] = {str(k): v for k, v in USER_CAPTION_CONFIG.items()}
    full_config['user_language_config'] = {str(k): v for k, v in USER_LANGUAGE_CONFIG.items()} 
    with open(CONFIG_FILE, 'w') as f: json.dump(full_config, f, indent=4)
    print(f"✅ Config saved.")

def get_user_inputs():
    print("--- 🔐 Bot Configuration ---")
    bot_token = input("1. Bot Token: ").strip()
    api_id = input("2. API ID: ").strip()
    api_hash = input("3. API Hash: ").strip()
    target_chat_id = input("4. Target Chat ID: ").strip()
    try: api_id = int(api_id)
    except: return get_user_inputs()
    return {"bot_token": bot_token, "api_id": api_id, "api_hash": api_hash, "target_chat_id": target_chat_id}

def parse_caption_args(args: list) -> dict | None:
    config = {'e_current': 1, 'e_max': 999, 's_val': '01', 'q_list': [], 'q_index': 0, 'e2_current': None, 'e2_parenthesis': None, 'enabled': False}
    if not args or args[0].lower() not in ['on', 'off']: return None
    config['enabled'] = args[0].lower() == 'on'
    if not config['enabled']: return config
    temp_args = args[1:]; i = 0
    while i < len(temp_args):
        cmd = temp_args[i].lower(); i += 1
        if cmd == 'e':
            if i < len(temp_args):
                val = temp_args[i]; i += 1
                if val.startswith('(') and val.endswith(')'):
                    try: config['e2_parenthesis'] = val[1:-1]; config['e2_current'] = int(val[1:-1])
                    except: return None
                else:
                    try: config['e_current'] = int(val)
                    except: return None
        elif cmd == 's':
            if i < len(temp_args): config['s_val'] = temp_args[i]; i += 1
        elif cmd == 'en':
            if i < len(temp_args): 
                try: config['e_max'] = int(temp_args[i]); i += 1
                except: return None
        elif cmd == 'q':
            while i < len(temp_args) and temp_args[i].lower() not in ['e', 's', 'en', 'q']:
                config['q_list'].append(temp_args[i]); i += 1
    if config['enabled'] and not config['q_list']: return None
    return config

def generate_caption_and_update_state(user_id: int, total_videos_uploaded: int) -> tuple[dict, list[str]]:
    if user_id not in USER_CAPTION_CONFIG or not USER_CAPTION_CONFIG[user_id]['enabled']:
        return USER_CAPTION_CONFIG.get(user_id, {'enabled': False}), [None] * total_videos_uploaded
    config = USER_CAPTION_CONFIG[user_id]
    captions = []
    q_len = len(config['q_list'])
    lang_type = USER_LANGUAGE_CONFIG.get(user_id, 'fandub') 
    language_line = f"**🎧 Language - Hindi ({'OFFICIAL' if lang_type == 'official' else 'Fan Dub'})**"
    e_current_start = config['e_current']
    e2_current_start = config['e2_current']
    q_index_start = config['q_index']
    for i in range(total_videos_uploaded):
        cycle_increment = math.floor((q_index_start + i) / q_len)
        current_e = e_current_start + cycle_increment
        current_q_index = (q_index_start + i) % q_len
        episode_line = f"**✔️ Episode - {current_e:02d}**"
        if config['e2_parenthesis'] is not None:
            current_e2 = (e2_current_start or 0) + cycle_increment
            episode_line += f" **({current_e2:02d})**"
        if config['s_val']: episode_line += f" **(S{config['s_val']})**"
        if current_e == config['e_max']: episode_line += " **End**"
        quality_line = f"**⚡ Quality : {config['q_list'][current_q_index]}**"
        captions.append(f"{episode_line}\n{language_line}\n{quality_line}")
    total_cycles = math.floor((q_index_start + total_videos_uploaded) / q_len)
    config['e_current'] += total_cycles
    config['q_index'] = (q_index_start + total_videos_uploaded) % q_len
    if config['e2_current'] is not None: config['e2_current'] += total_cycles
    return config, captions

async def upload_single_video(client, video_path, user_id, TARGET_CHAT, progress_callback, caption_text, unique_id):
    MAX_FILE_SIZE = 2097152000 
    if video_path.stat().st_size > MAX_FILE_SIZE: print("❌ File too large."); return None
    metadata = get_video_metadata(video_path)
    thumb_path = None; cleanup = None
    if user_id in USER_THUMBS: thumb_path = USER_THUMBS[user_id]
    elif str(user_id) in USER_THUMB_TIME:
        ts = USER_THUMB_TIME[str(user_id)]
        out = TMP / f"thumb_{user_id}_{unique_id}_{ts}s.jpg"
        if not (out.exists() and out.stat().st_size > 0):
            if await asyncio.to_thread(generate_video_thumbnail, video_path, out, int(ts)): thumb_path = str(out); cleanup = out
            else: dummy = TMP/"d.jpg"; create_dummy_thumb(str(dummy)); thumb_path = str(dummy)
        else: thumb_path = str(out); cleanup = out
    
    cap = caption_text
    if cap is None:
        lang = USER_LANGUAGE_CONFIG.get(user_id, 'fandub')
        l_line = f"**🎧 Language - Hindi ({'OFFICIAL' if lang == 'official' else 'Fan Dub'})**" 
        cap = f"**Upload: {video_path.name}**\n{l_line}\n**Res: {metadata['width']}x{metadata['height']}**\n**Dur: {metadata['duration']}s**"
    try:
        await client.send_video(chat_id=TARGET_CHAT, video=str(video_path), thumb=thumb_path, caption=cap, progress=progress_callback, parse_mode=ParseMode.MARKDOWN, duration=metadata['duration'], width=metadata['width'], height=metadata['height'])
        print("\n✅ Uploaded.")
    except Exception as e: logger.error(f"Upload Error: {e}")
    return cleanup 

# --- YOUTUBE DOWNLOAD & TG UPLOAD LOGIC ---
async def run_youtube_downloader(is_tg_upload: bool, client: Client, user_id: int, target_chat: str, progress_callback):
    while True:
        download_queue = load_queue()
        print("\n" + "★"*45)
        print("      TA HD YOU TUBE VIDEO DOWNLOADER      ")
        print("★"*45)
        
        if download_queue:
            print(f"\n[Queue Status]: {len(download_queue)} items ready in list.")
            print("Type 'ok' to start downloading all.")

        url = input("\nEnter YouTube URL (or 'e' to exit, 'ok' to start): ").strip()
        
        if url.lower() in ['e', 'exit']:
            print("Goodbye!")
            break
        
        if url.lower() == 'ok':
            if not download_queue:
                print("\nQueue is empty! Add links first.")
                continue
            
            total_videos = sum(len(task['choices']) for task in download_queue)
            up_conf = None
            caps = []
            if is_tg_upload and total_videos > 0:
                up_conf, caps = generate_caption_and_update_state(user_id, total_videos)
            current_video_idx = 0
            
            for task in download_queue:
                task_url = task['url']
                choices = task['choices']
                
                for choice in choices:
                    format_str = get_quality_format(choice)
                    dl_path = '/content/downloads' if IS_COLAB else '/sdcard/Download'
                    os.makedirs(dl_path, exist_ok=True)
                    
                    ydl_opts = {
                        'format': format_str,
                        'outtmpl': f'{dl_path}/%(title)s.%(ext)s',
                        'noplaylist': True,
                        'quiet': True,
                        'no_warnings': True,
                        'noprogress': True,
                        'progress_hooks': [progress_hook],
                        'concurrent_fragment_downloads': 4,
                    }

                    if choice == '8':
                        ydl_opts['postprocessors'] = [{
                            'key': 'FFmpegExtractAudio',
                            'preferredcodec': 'mp3',
                            'preferredquality': '192',
                        }]

                    try:
                        ydl = yt_dlp.YoutubeDL(ydl_opts)
                        info = ydl.extract_info(task_url, download=False)
                        filename = ydl.prepare_filename(info)
                        
                        if choice == '8':
                            base, _ = os.path.splitext(filename)
                            filename = base + ".mp3"

                        if os.path.exists(filename):
                            base, ext = os.path.splitext(filename)
                            counter = 1
                            new_filename = f"{base} ({counter:02d}){ext}"
                            while os.path.exists(new_filename):
                                counter += 1
                                new_filename = f"{base} ({counter:02d}){ext}"
                            ydl_opts['outtmpl'] = new_filename
                            ydl = yt_dlp.YoutubeDL(ydl_opts)
                            filename = new_filename
                        
                        print(f"\n[TA HD] Downloading: {info.get('title')} [{choice}]")
                        ydl.process_ie_result(info, download=True)
                        print("\n✅ Download Completed!")
                        
                        if is_tg_upload:
                            f_path = Path(filename)
                            if f_path.exists():
                                print(f"\n--- Uploading {f_path.name} to Telegram ---")
                                uid = uuid.uuid4().hex
                                audio_info = get_audio_stream_info(f_path)
                                has_opus = any(s.get('codec', '').lower() == 'opus' for s in audio_info)
                                
                                if has_opus or f_path.suffix.lower() == '.mkv': target_ext = ".mkv"
                                elif f_path.suffix.lower() == '.mp3': target_ext = ".mp3"
                                else: target_ext = ".mp4"
                                
                                new_n = f"{FIXED_RENAME_PREFIX}{target_ext}"
                                tmp_up = TMP / new_n
                                dur = get_video_metadata(f_path)['duration']
                                cleanup = [f_path] 
                                
                                try:
                                    await process_metadata_and_rename(f_path, tmp_up, dur)
                                    cleanup.append(tmp_up)
                                    current_cap = caps[current_video_idx] if caps else None
                                    th = await upload_single_video(client, tmp_up, user_id, target_chat, progress_callback, current_cap, uid)
                                    if th: cleanup.append(th)
                                except Exception as e:
                                    print(f"❌ Upload Error {f_path.name}: {e}")
                                    
                                for x in cleanup: 
                                    try: os.remove(x)
                                    except: pass
                                    
                                current_video_idx += 1

                    except Exception as e:
                        print(f"\n❌ Error: {e}")
                        if is_tg_upload:
                            current_video_idx += 1
        
            if is_tg_upload and up_conf and up_conf['enabled']:
                USER_CAPTION_CONFIG[user_id] = up_conf
                save_config(GLOBAL_CONFIG)

            if os.path.exists('queue.json'):
                os.remove('queue.json')
        
            input("\nAll tasks finished. Press Enter to clear...")
            clear_screen()
            continue

        print("\nSelect Quality (Multiple allowed, e.g., 4,2,0):")
        print("0. Best Quality (Auto)  4. 480p (Medium)    7. 144p (Lowest)")
        print("1. 1080p (Full HD)      5. 360p (Standard)  8. Only Audio (MP3)")
        print("2. 720p (HD)            6. 240p (Low)       c. Cancel")
        print("3. 560p (Custom)")
        
        choice_input = input("\nChoice (0-8/c/Enter for auto): ").lower().strip()
        
        if choice_input == 'c': 
            clear_screen()
            continue

        final_choices = []
        if choice_input == "":
            final_choices = download_queue[0]['choices'] if download_queue else ['0']
        elif choice_input.endswith('l') and choice_input[:-1].isdigit():
            idx = int(choice_input[:-1]) - 1
            final_choices = download_queue[idx]['choices'] if (download_queue and 0 <= idx < len(download_queue)) else ['0']
        else:
            final_choices = [c.strip() for c in choice_input.split(',') if c.strip() in '012345678']

        if final_choices:
            download_queue.append({'url': url, 'choices': final_choices})
            save_queue(download_queue)
            print(f"\n[+] Added to queue. (Total: {len(download_queue)})")
        else:
            print("\nInvalid selection!")
# ----------------------------------------

async def command_mode(client: Client):
    global GLOBAL_CONFIG
    print("\n--- 💻 Command Mode ---")
    print("Cmds: set_thum <path/time> | del_thum | set_cap | upload | upload_mkv | convert | youtube or yt | youtubetg or ytg")
    TARGET_CHAT = GLOBAL_CONFIG.get('target_chat_id', 'me')
    user_id = CLI_USER_ID 
    def progress_callback(current, total, *args):
        if not hasattr(progress_callback, 'pbar'): progress_callback.pbar = tqdm(total=total, unit="B", unit_scale=True, desc="Upload", dynamic_ncols=True)
        progress_callback.pbar.update(current - progress_callback.pbar.n)
        if current == total: progress_callback.pbar.close(); del progress_callback.pbar

    while True:
        try:
            line = input("Bot Command> ").strip()
            if line.lower() in ['exit', 'e']: save_config(GLOBAL_CONFIG); break
            parts = line.split(); 
            if not parts: continue
            cmd = parts[0].lower(); args = parts[1:]

            if cmd in ['set_thum', 'thum']:
                if len(args) == 1:
                    arg = " ".join(args).strip()
                    seconds = parse_time(arg)
                    if seconds > 0:
                        USER_THUMB_TIME[str(user_id)] = seconds
                        USER_THUMBS.pop(user_id, None)
                        save_config(GLOBAL_CONFIG)
                        print(f"✅ Time set: {seconds}s")
                    else:
                        path_arg = Path(os.path.expanduser(arg))
                        sel_img = interactive_file_explorer(path_arg, IMAGE_EXTENSIONS, folder_select_mode=False)
                        if sel_img:
                             out_path = TMP / f"thumb_{user_id}_manual.jpg"
                             try:
                                 img = Image.open(sel_img).convert("RGB")
                                 img.thumbnail((320, 320))
                                 img.save(out_path, "JPEG")
                                 USER_THUMBS[user_id] = str(out_path)
                                 USER_THUMB_TIME.pop(str(user_id), None)
                                 save_config(GLOBAL_CONFIG)
                                 print(f"✅ Photo set: {sel_img.name}")
                             except Exception as e: print(f"❌ Image error: {e}")
                        else: print("❌ Cancelled.")
                else: print("❌ Usage: set_thum <time/path>")

            elif cmd == 'del_thum':
                if user_id in USER_THUMBS: del USER_THUMBS[user_id]
                if str(user_id) in USER_THUMB_TIME: del USER_THUMB_TIME[str(user_id)]
                save_config(GLOBAL_CONFIG); print("✅ Thumbnails cleared.")

            elif cmd in ['set_cap', 'cap']:
                if len(args) == 1 and args[0].lower() in ['o', 'f']:
                    USER_LANGUAGE_CONFIG[user_id] = 'official' if args[0].lower() == 'o' else 'fandub'
                    save_config(GLOBAL_CONFIG); print(f"✅ Language: {USER_LANGUAGE_CONFIG[user_id].upper()}")
                    continue 
                new_conf = parse_caption_args(args)
                if new_conf:
                    USER_CAPTION_CONFIG.pop(user_id, None)
                    if new_conf['enabled']: USER_CAPTION_CONFIG[user_id] = new_conf
                    save_config(GLOBAL_CONFIG); print(f"✅ Caption Updated.")
                else: print("❌ Invalid args.")

            elif cmd in ['upload', 'up']:
                if len(args) != 1: print("❌ Need path."); continue
                p_arg = Path(os.path.expanduser(args[0]))
                selected_path = interactive_file_explorer(p_arg, VIDEO_EXTENSIONS, folder_select_mode=True)
                
                if not selected_path: print("❌ Cancelled."); continue

                files = []
                if selected_path.is_file(): files.append(selected_path)
                else:
                     all_v = sorted([f for f in selected_path.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS])
                     if not all_v: print("⚠️ No videos."); continue
                     print(f"\n📁 {selected_path.name}:")
                     f_map = {i+1: f for i, f in enumerate(all_v)}
                     for i, f in f_map.items(): print(f" {i}> {f.name}")
                     sel = input("Select (e.g. 1,3, 5-8): ").strip()
                     idxs = parse_range_selection(sel)
                     if not idxs: print("⚠️ None selected."); continue
                     for x in idxs:
                         if x in f_map: files.append(f_map[x])

                if files:
                    up_conf, caps = generate_caption_and_update_state(user_id, len(files))
                    cleanup = []
                    for i, f in enumerate(files):
                        uid = uuid.uuid4().hex
                        audio_info = get_audio_stream_info(f)
                        has_opus = any(s.get('codec', '').lower() == 'opus' for s in audio_info)
                        target_ext = ".mkv" if (has_opus or f.suffix.lower() == '.mkv') else ".mp4"

                        new_n = f"{FIXED_RENAME_PREFIX}{target_ext}"
                        tmp_up = TMP / new_n
                        print(f"\n--- Processing {f.name} -> {new_n} ---")
                        dur = get_video_metadata(f)['duration']
                        try:
                            await process_metadata_and_rename(f, tmp_up, dur)
                            cleanup.append(tmp_up)
                            th = await upload_single_video(client, tmp_up, user_id, TARGET_CHAT, progress_callback, caps[i] if caps else None, uid)
                            if th: cleanup.append(th)
                        except Exception as e: print(f"❌ Error {f.name}: {e}")
                    if up_conf['enabled']: USER_CAPTION_CONFIG[user_id] = up_conf; save_config(GLOBAL_CONFIG)
                    for x in cleanup: 
                        try: os.remove(x)
                        except: pass

            elif cmd == 'upload_mkv':
                if len(args) != 1: print("❌ Need path."); continue
                p_arg = Path(os.path.expanduser(args[0]))
                selected_path = interactive_file_explorer(p_arg, VIDEO_EXTENSIONS, folder_select_mode=True)
                
                if not selected_path: print("❌ Cancelled."); continue

                files = []
                if selected_path.is_file(): files.append(selected_path)
                else:
                    all_v = sorted([f for f in selected_path.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS])
                    if not all_v: print("⚠️ No videos."); continue
                    print(f"\n📁 {selected_path.name}:")
                    f_map = {i+1: f for i, f in enumerate(all_v)}
                    for i, f in f_map.items(): print(f" {i}> {f.name}")
                    sel = input("Select (e.g. 1,3, 5-8): ").strip()
                    idxs = parse_range_selection(sel)
                    for x in idxs:
                         if x in f_map: files.append(f_map[x])

                if files:
                    up_conf, caps = generate_caption_and_update_state(user_id, len(files))
                    cleanup = []
                    for i, f in enumerate(files):
                        print(f"\n--- Checking {f.name} ---")
                        a_s = get_audio_stream_info(f)
                        cur_p = f; dur = get_video_metadata(f)['duration']
                        has_opus = any(s.get('codec', '').lower() == 'opus' for s in a_s)
                        target_ext = ".mkv" if (has_opus or f.suffix.lower() == '.mkv') else ".mp4"
                            
                        try:
                            if len(a_s) <= 1:
                                new_n = f"{FIXED_RENAME_PREFIX}{target_ext}"
                                tmp_up = TMP / new_n
                                await process_metadata_and_rename(f, tmp_up, dur)
                                cur_p = tmp_up; cleanup.append(tmp_up)
                            else:
                                for ix, tr in enumerate(a_s): print(f" [{ix+1}] {tr['description']}")
                                order = input("Order (3,2,1) or Enter skip: ").strip()
                                if not order:
                                    new_n = f"{FIXED_RENAME_PREFIX}{target_ext}"
                                    tmp_up = TMP / new_n
                                    await process_metadata_and_rename(f, tmp_up, dur)
                                    cur_p = tmp_up; cleanup.append(tmp_up)
                                else:
                                    map_idx = [int(x) for x in order.split(',') if x.strip().isdigit()]
                                    if map_idx:
                                        sel_opus = False
                                        for idx in map_idx:
                                            if idx - 1 < len(a_s) and a_s[idx-1]['codec'].lower() == 'opus':
                                                sel_opus = True; break
                                        
                                        t_ext = ".mkv" if (sel_opus or f.suffix.lower() == '.mkv') else ".mp4"
                                        new_n = f"{FIXED_RENAME_PREFIX}{t_ext}"
                                        tmp_up = TMP / new_n
                                        await modify_audio_tracks_and_copy(f, tmp_up, map_idx, dur)
                                        cur_p = tmp_up; cleanup.append(tmp_up)
                                    else: print("❌ Invalid order."); continue
                            th = await upload_single_video(client, cur_p, user_id, TARGET_CHAT, progress_callback, caps[i] if caps else None, uuid.uuid4().hex)
                            if th: cleanup.append(th)
                        except Exception as e: print(f"❌ Error: {e}")
                    if up_conf['enabled']: USER_CAPTION_CONFIG[user_id] = up_conf; save_config(GLOBAL_CONFIG)
                    for x in cleanup: 
                        try: os.remove(x)
                        except: pass

            elif cmd == 'convert':
                if not args: print("❌ Usage: convert [tg] <path>"); continue
                is_up = args[0].lower() == 'tg'
                p_arg = Path(os.path.expanduser(args[-1]))
                selected_path = interactive_file_explorer(p_arg, VIDEO_EXTENSIONS, folder_select_mode=True)
                
                if not selected_path: print("❌ Cancelled."); continue

                files = []
                if selected_path.is_file(): files.append(selected_path)
                else:
                    all_v = sorted([f for f in selected_path.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS])
                    f_map = {i+1: f for i, f in enumerate(all_v)}
                    for i, f in f_map.items(): print(f" {i}> {f.name}")
                    sel = input("Select (e.g. 1,3, 5-8): ").strip()
                    idxs = parse_range_selection(sel)
                    for x in idxs:
                         if x in f_map: files.append(f_map[x])
                
                if not files: continue
                t_sz = input("Target Size (e.g. 100MB): ").strip()
                t_b = parse_size(t_sz)
                if t_b == 0: continue
                up_conf, caps = generate_caption_and_update_state(user_id, len(files))
                cleanup = []
                for i, f in enumerate(files):
                    dur = get_video_metadata(f)['duration']
                    if dur == 0: continue
                    bit = math.ceil(((t_b * 8) / dur) / 1000)
                    c_out = f.parent / f"compressed_{f.name}"
                    if is_up: c_out = TMP / f"compressed_{f.name}"
                    if await compress_video(f, c_out, bit, dur):
                        if is_up:
                            cleanup.append(c_out)
                            target_ext = '.mkv' if f.suffix.lower() == '.mkv' else '.mp4'
                            f_up = TMP / f"{FIXED_RENAME_PREFIX}{target_ext}"
                            await process_metadata_and_rename(c_out, f_up, dur)
                            cleanup.append(f_up)
                            th = await upload_single_video(client, f_up, user_id, TARGET_CHAT, progress_callback, caps[i] if caps else None, uuid.uuid4().hex)
                            if th: cleanup.append(th)
                    else: print("❌ Fail.")
                if up_conf['enabled']: USER_CAPTION_CONFIG[user_id] = up_conf; save_config(GLOBAL_CONFIG)
                for x in cleanup: 
                    try: os.remove(x)
                    except: pass
            
            elif cmd in ['youtube', 'yt']:
                await run_youtube_downloader(is_tg_upload=False, client=client, user_id=user_id, target_chat=TARGET_CHAT, progress_callback=progress_callback)
            
            elif cmd in ['youtubetg', 'ytg']:
                await run_youtube_downloader(is_tg_upload=True, client=client, user_id=user_id, target_chat=TARGET_CHAT, progress_callback=progress_callback)
            
            else: print("❌ Unknown.")
        except Exception as e: logger.error(f"Err: {e}")

def main():
    c = load_config()
    if not c or not c.get('bot_token') or not c.get('target_chat_id'): 
        if IS_COLAB:
            print("❌ ERROR: Please set API_ID, API_HASH, BOT_TOKEN, and TARGET_CHAT_ID in Colab Secrets!")
            return
        c = get_user_inputs()
    
    global GLOBAL_CONFIG; GLOBAL_CONFIG = c
    if not IS_COLAB: save_config(c)
    
    print(f"\nTarget: {c.get('target_chat_id')}")
    try: 
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
        asyncio.run(run_client(c))
    except KeyboardInterrupt: print("\n👋 Bye.")

async def run_client(c):
    if not c['bot_token']: return
    app = Client("my_session", api_id=c['api_id'], api_hash=c['api_hash'], bot_token=c['bot_token'])
    async with app: logger.info("🟢 Online."); await command_mode(app)

if __name__ == "__main__":
    main()

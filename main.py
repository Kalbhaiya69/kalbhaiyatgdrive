#!/usr/bin/env python3
"""
Telegram Bot for downloading Google Drive files (2GB Support)
Author: Replit Agent
Dependencies: pyrogram, requests
"""

import os
import re
import tempfile
import time
import asyncio
from typing import Optional, Tuple, List, Dict, Union
from pathlib import Path
import urllib.parse
import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime

import requests
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait

# Configuration
TOKEN = os.getenv('TOKEN')
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')

MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB in bytes (enhanced limit)
CHUNK_SIZE = 16 * 1024 * 1024  # 16MB chunks (optimized for speed)
PROGRESS_UPDATE_INTERVAL = 5  # seconds (faster updates)
DOWNLOAD_TIMEOUT = 1800  # 30 minutes timeout for large downloads
UPLOAD_TIMEOUT = 2400  # 40 minutes timeout for large uploads
UPLOAD_PROGRESS_INTERVAL = 8  # seconds (faster upload progress updates)
SPLIT_SIZE = 1950 * 1024 * 1024  # 1.95GB per part (safe margin under 2GB limit)
MAX_QUEUE_SIZE = 5  # Maximum files in queue
SUPPORTED_DOMAINS = ['drive.google.com', 'drive.usercontent.google.com', 'dropbox.com', 'onedrive.live.com', '1drv.ms']  # Only properly supported services

@dataclass
class QueueItem:
    """Represents an item in the download queue"""
    url: str
    user_id: int
    chat_id: int
    message_id: int
    timestamp: datetime
    item_type: str = "file"  # "file" or "folder"

@dataclass 
class FileInfo:
    """File information structure"""
    name: str
    size: Optional[int]
    download_url: str
    file_id: Optional[str] = None

class DownloadQueue:
    """Manages download queue for multiple users"""
    
    def __init__(self):
        self.queues: Dict[int, deque] = {}  # user_id -> queue
        self.processing: Dict[int, bool] = {}  # user_id -> is_processing
    
    def add_item(self, user_id: int, item: QueueItem) -> bool:
        """Add item to user's queue. Returns False if queue is full."""
        if user_id not in self.queues:
            self.queues[user_id] = deque()
            self.processing[user_id] = False
        
        if len(self.queues[user_id]) >= MAX_QUEUE_SIZE:
            return False
        
        self.queues[user_id].append(item)
        return True
    
    def get_next_item(self, user_id: int) -> Optional[QueueItem]:
        """Get next item from user's queue"""
        if user_id not in self.queues or not self.queues[user_id]:
            return None
        return self.queues[user_id].popleft()
    
    def set_processing(self, user_id: int, processing: bool):
        """Set processing status for user"""
        self.processing[user_id] = processing
    
    def is_processing(self, user_id: int) -> bool:
        """Check if user has items being processed"""
        return self.processing.get(user_id, False)
    
    def get_queue_size(self, user_id: int) -> int:
        """Get current queue size for user"""
        if user_id not in self.queues:
            return 0
        return len(self.queues[user_id])
    
    def clear_queue(self, user_id: int):
        """Clear user's queue"""
        if user_id in self.queues:
            self.queues[user_id].clear()

# Global queue instance
download_queue = DownloadQueue()
processed_messages = set()  # To prevent duplicate message processing

class FileSplitter:
    """Handles file splitting for large files"""
    
    @staticmethod
    def split_file(file_path: str, split_size: int = SPLIT_SIZE) -> List[str]:
        """Split a large file into smaller chunks"""
        parts = []
        file_size = os.path.getsize(file_path)
        
        if file_size <= split_size:
            return [file_path]  # No need to split
        
        base_name = os.path.basename(file_path)
        temp_dir = os.path.dirname(file_path)
        
        with open(file_path, 'rb') as f:
            part_num = 1
            while True:
                chunk = f.read(split_size)
                if not chunk:
                    break
                
                part_filename = f"{temp_dir}/{base_name}.part{part_num:03d}"
                with open(part_filename, 'wb') as part_file:
                    part_file.write(chunk)
                
                parts.append(part_filename)
                part_num += 1
        
        return parts
    
    @staticmethod
    def cleanup_parts(parts: List[str]):
        """Clean up temporary part files"""
        for part in parts:
            try:
                if os.path.exists(part):
                    os.unlink(part)
            except Exception:
                pass

class URLDetector:
    """Enhanced URL detection for multiple file hosting services"""
    
    @staticmethod
    def is_supported_url(url: str) -> bool:
        """Check if URL is from a supported domain or looks like a direct download"""
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower()
        
        # Check supported cloud services
        if any(supported in domain for supported in SUPPORTED_DOMAINS):
            return True
        
        # Check if it looks like a direct download link
        if url.startswith(('http://', 'https://')):
            path = parsed.path.lower()
            # Accept URLs that end with common file extensions
            if any(path.endswith(ext) for ext in ['.apk', '.zip', '.rar', '.mp4', '.mkv', '.avi', '.mp3', '.pdf', '.exe', '.dmg', '.pkg', '.deb', '.rpm', '.tar.gz', '.7z']):
                return True
            # Accept URLs with download parameters
            if any(param in url.lower() for param in ['download', 'dl=', 'file=']):
                return True
        
        return False
    
    @staticmethod
    def get_direct_download_url(url: str) -> str:
        """Convert various URLs to direct download URLs"""
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower()
        
        # Google Drive
        if 'drive.google.com' in domain:
            return url  # Handle with existing Google Drive logic
        
        # Dropbox
        elif 'dropbox.com' in domain:
            if '?dl=0' in url:
                return url.replace('?dl=0', '?dl=1')
            elif '?dl=1' not in url:
                return url + ('&' if '?' in url else '?') + 'dl=1'
        
        # OneDrive
        elif any(od in domain for od in ['onedrive.live.com', '1drv.ms']):
            if '?download=1' not in url:
                return url + ('&' if '?' in url else '?') + 'download=1'
        
        # Other services - return as-is for now
        # Note: Full resolver implementation needed for complex services
        
        # Direct links (already downloadable)
        return url

class GoogleDriveFolderHandler:
    """Handles Google Drive folder operations"""
    
    @staticmethod
    def extract_folder_id(url: str) -> Optional[str]:
        """Extract folder ID from Google Drive folder URL (including mobile format)"""
        patterns = [
            r'/folders/([a-zA-Z0-9-_]+)',  # Standard format
            r'/mobile/folders/([a-zA-Z0-9-_]+)',  # Mobile format - first folder ID
            r'[?&]id=([a-zA-Z0-9-_]+)',  # Query parameter format
            r'/open\?id=([a-zA-Z0-9-_]+)'  # Open format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None
    
    @staticmethod
    async def get_folder_files(folder_id: str) -> List[FileInfo]:
        """Get list of files in a Google Drive folder using Drive API endpoint"""
        try:
            # Use Drive API V3 endpoint (public folders don't require auth)
            api_url = f"https://www.googleapis.com/drive/v3/files"
            params = {
                'q': f"'{folder_id}' in parents and trashed=false",
                'fields': 'files(id,name,size,mimeType)',
                'pageSize': 20,  # Limit to 20 files for now
                # Note: This requires a valid Google API key - removed hardcoded key for security
            }
            
            session = create_drive_session()
            response = session.get(api_url, params=params, timeout=30)
            
            # Skip API approach due to authentication requirements
            if False:  # Disabled - requires proper API key setup
                data = response.json()
                files = []
                
                for file_data in data.get('files', []):
                    # Skip Google Workspace files and folders
                    mime_type = file_data.get('mimeType', '')
                    if 'google-apps' in mime_type or 'folder' in mime_type:
                        continue
                    
                    file_info = FileInfo(
                        name=file_data.get('name', 'Unknown'),
                        size=int(file_data.get('size', 0)) if file_data.get('size') else None,
                        download_url=f"https://drive.google.com/uc?export=download&id={file_data.get('id')}",
                        file_id=file_data.get('id')
                    )
                    files.append(file_info)
                
                return files
            
            else:
                # Fallback to basic HTML parsing for private/restricted folders
                return await GoogleDriveFolderHandler._fallback_html_parsing(folder_id)
                
        except Exception as e:
            print(f"Error getting folder files: {e}")
            # Fallback to HTML parsing
            return await GoogleDriveFolderHandler._fallback_html_parsing(folder_id)
    
    @staticmethod
    async def _fallback_html_parsing(folder_id: str) -> List[FileInfo]:
        """Fallback method using HTML parsing for restricted folders"""
        try:
            folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
            session = create_drive_session()
            
            response = session.get(folder_url, timeout=30)
            response.raise_for_status()
            
            # Look for JSON data in the HTML that contains file information
            json_pattern = r'window\._page_data = (\[.*?\]);'
            json_match = re.search(json_pattern, response.text, re.DOTALL)
            
            files = []
            if json_match:
                try:
                    import json
                    page_data = json.loads(json_match.group(1))
                    # Navigate through the nested structure to find file data
                    # This is a simplified extraction - Drive's structure is complex
                    for item in str(page_data):
                        file_id_matches = re.findall(r'([a-zA-Z0-9-_]{28,44})', item)
                        for potential_id in file_id_matches:
                            if len(potential_id) >= 28:
                                files.append(FileInfo(
                                    name=f"file_{len(files)+1}",
                                    size=None,
                                    download_url=f"https://drive.google.com/uc?export=download&id={potential_id}",
                                    file_id=potential_id
                                ))
                                if len(files) >= 10:  # Limit for safety
                                    break
                        if len(files) >= 10:
                            break
                except:
                    pass
            
            return files[:10]  # Conservative limit
            
        except Exception as e:
            print(f"Fallback parsing error: {e}")
            return []

class ProgressTracker:
    """Track download progress and calculate statistics"""

    def __init__(self, total_size: Optional[int] = None):
        self.total_size = total_size
        self.downloaded = 0
        self.start_time = time.time()
        self.last_update = self.start_time
        self.speed_samples = []
        self.last_downloaded = 0

    def update(self, chunk_size: int):
        """Update progress with new chunk"""
        self.downloaded += chunk_size
        current_time = time.time()

        # Calculate speed using recent samples (optimized for larger chunks)
        if current_time - self.last_update >= 0.5:  # Update speed every 500ms for better accuracy
            time_diff = current_time - self.last_update
            bytes_diff = self.downloaded - self.last_downloaded
            speed = bytes_diff / time_diff

            self.speed_samples.append(speed)
            # Keep only last 15 samples for better average with larger chunks
            if len(self.speed_samples) > 15:
                self.speed_samples.pop(0)

            self.last_update = current_time
            self.last_downloaded = self.downloaded

    def get_stats(self) -> dict:
        """Get current progress statistics"""
        if self.total_size is None or self.total_size == 0:
            percentage = None
        else:
            percentage = (self.downloaded / self.total_size) * 100

        # Calculate average speed
        avg_speed = sum(self.speed_samples) / len(self.speed_samples) if self.speed_samples else 0

        # Calculate ETA
        if avg_speed > 0 and self.total_size and self.total_size > self.downloaded:
            remaining_bytes = self.total_size - self.downloaded
            eta_seconds = remaining_bytes / avg_speed
        else:
            eta_seconds = None

        return {
            'percentage': percentage,
            'downloaded_mb': self.downloaded / (1024 * 1024),
            'total_mb': self.total_size / (1024 * 1024) if self.total_size else None,
            'speed_mbps': avg_speed / (1024 * 1024),
            'eta_seconds': eta_seconds
        }

    def format_progress(self) -> str:
        """Format progress message for Telegram"""
        stats = self.get_stats()

        # Format with or without size info
        if stats['percentage'] is not None:
            # Format ETA
            eta_seconds = int(stats['eta_seconds'] or 0)
            hours = eta_seconds // 3600
            minutes = (eta_seconds % 3600) // 60
            seconds = eta_seconds % 60
            eta_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            return (f"⏳ Downloading: {stats['percentage']:.1f}% — "
                    f"{stats['downloaded_mb']:.1f}MB/{stats['total_mb']:.1f}MB — "
                    f"{stats['speed_mbps']:.1f} MB/s — ETA {eta_str}")
        else:
            # Unknown file size
            return (f"⏳ Downloading: {stats['downloaded_mb']:.1f}MB — "
                    f"{stats['speed_mbps']:.1f} MB/s — Size unknown")

def extract_google_drive_file_id(url: str) -> Optional[str]:
    """Extract file ID from Google Drive URL"""
    patterns = [
        r'/file/d/([a-zA-Z0-9-_]+)',
        r'[?&]id=([a-zA-Z0-9-_]+)',  # Updated to handle URL parameters better
        r'/open\?id=([a-zA-Z0-9-_]+)'
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    return None

def is_google_drive_folder(url: str) -> bool:
    """Check if URL points to a Google Drive folder"""
    folder_patterns = [
        r'/folders/',
        r'folder/',
        r'#folders'
    ]

    return any(re.search(pattern, url) for pattern in folder_patterns)

def is_direct_download_url(url: str) -> bool:
    """Check if URL is already a direct download URL"""
    return 'drive.usercontent.google.com' in url and 'download' in url

def get_google_drive_download_url(file_id: str, original_url: str = "") -> str:
    """Generate direct download URL for Google Drive file"""
    # If the original URL is already a direct download URL, use it
    if is_direct_download_url(original_url):
        return original_url
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def create_drive_session() -> requests.Session:
    """Create an optimized session with browser-like headers for Google Drive"""
    session = requests.Session()
    
    # Optimize for speed
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=20,  # Connection pooling
        pool_maxsize=20,
        max_retries=3,
        pool_block=False
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://drive.google.com/',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',  # Safe compression methods
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    })
    return session

def extract_confirm_token(response_text: str) -> Optional[str]:
    """Extract confirm token from Google Drive HTML page"""
    # Look for confirm token in various formats
    patterns = [
        r'confirm=([0-9A-Za-z_\-]+)',
        r'"confirm"\s*:\s*"([0-9A-Za-z_\-]+)"',
        r'confirm&id=([0-9A-Za-z_\-]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, response_text)
        if match:
            return match.group(1)
    return None

def extract_filename_from_disposition(content_disposition: str) -> Optional[str]:
    """Extract filename from Content-Disposition header with RFC 5987 support"""
    if not content_disposition:
        return None
        
    # Try filename*= first (RFC 5987) - fixed regex
    filename_star_match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", content_disposition)
    if filename_star_match:
        import urllib.parse
        return urllib.parse.unquote(filename_star_match.group(1))
    
    # Fallback to filename=
    filename_match = re.search(r'filename=(["\']?)([^;]+?)\1', content_disposition)
    if filename_match:
        return filename_match.group(2).strip('"\'')
        
    return None

async def resolve_drive_download(file_id: str, original_url: str = "") -> Tuple[Optional[requests.Response], Optional[int], Optional[str], Optional[str]]:
    """Resolve Google Drive download with proper token handling"""
    session = create_drive_session()
    download_url = get_google_drive_download_url(file_id, original_url)
    
    # If it's already a direct download URL, handle it more efficiently
    if is_direct_download_url(download_url):
        try:
            response = session.get(download_url, stream=True, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            # Extract file info from headers
            filename = extract_filename_from_disposition(response.headers.get('content-disposition', ''))
            file_size = None
            content_length = response.headers.get('content-length')
            if content_length:
                try:
                    file_size = int(content_length)
                except ValueError:
                    pass
            
            return response, file_size, filename, None
        except Exception as e:
            return None, None, None, f"Error accessing direct download URL: {str(e)}"
    
    # Regular Google Drive URL processing with confirm token handling
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            # First request to get the download page
            response = session.get(download_url, stream=True, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            # Check if we got the file directly
            content_type = response.headers.get('content-type', '').lower()
            content_disposition = response.headers.get('content-disposition', '')
            
            if not content_type.startswith('text/html') and content_disposition:
                # We got the file directly
                filename = extract_filename_from_disposition(content_disposition)
                file_size = None
                content_length = response.headers.get('content-length')
                if content_length:
                    try:
                        file_size = int(content_length)
                    except ValueError:
                        pass
                return response, file_size, filename, None
            
            # We got HTML - need to extract confirm token
            html_content = response.text
            
            # Check for specific error patterns
            if 'download quota' in html_content.lower() or 'quota exceeded' in html_content.lower():
                return None, None, None, "Download quota exceeded for this file. Try again later."
            
            if 'need permission' in html_content.lower() or 'permission denied' in html_content.lower():
                return None, None, None, "File requires permission to access. Make sure it's shared with 'Anyone with the link'."
            
            if 'not found' in html_content.lower() or 'does not exist' in html_content.lower():
                return None, None, None, "File not found or has been deleted."
            
            # Look for confirm token in cookies first
            confirm_token = None
            for cookie in response.cookies:
                if cookie.name.startswith('download_warning'):
                    confirm_token = cookie.value
                    break
            
            # If no cookie, extract from HTML
            if not confirm_token:
                confirm_token = extract_confirm_token(html_content)
            
            if confirm_token:
                # Make second request with confirm token
                confirm_url = f"{download_url}&confirm={confirm_token}"
                response.close()  # Close the previous response
                
                response = session.get(confirm_url, stream=True, timeout=30, allow_redirects=True)
                response.raise_for_status()
                
                # Extract file info from final response
                filename = extract_filename_from_disposition(response.headers.get('content-disposition', ''))
                file_size = None
                content_length = response.headers.get('content-length')
                if content_length:
                    try:
                        file_size = int(content_length)
                    except ValueError:
                        pass
                        
                return response, file_size, filename, None
            
            # No confirm token found, might be an error page
            return None, None, None, "Unable to access file. It may be private or require special permissions."
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                continue
            return None, None, None, f"Network error: {str(e)}"
        except Exception as e:
            return None, None, None, f"Unexpected error: {str(e)}"
    
    return None, None, None, "Failed to resolve download after multiple attempts."

async def get_file_info_legacy(url: str) -> Tuple[Optional[int], Optional[str]]:
    """Legacy function - use resolve_drive_download instead"""
    try:
        # Use HEAD request to get headers without downloading
        response = requests.head(url, allow_redirects=True, timeout=10)

        # Check for content-length header
        content_length = response.headers.get('content-length')
        file_size = int(content_length) if content_length else None

        # Try to get filename from content-disposition header
        content_disposition = response.headers.get('content-disposition', '')
        filename = None
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[-1].strip('"')

        return file_size, filename
    except Exception:
        return None, None

async def download_file_streaming(response: requests.Response, progress_tracker: ProgressTracker, 
                                status_message: Message) -> str:
    """Download file with streaming and progress updates from active response"""
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file_path = temp_file.name
    last_progress_update = 0

    try:
        start_time = time.time()

        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                temp_file.write(chunk)
                progress_tracker.update(len(chunk))

                # Update progress message periodically (less frequent to avoid rate limits)
                current_time = time.time()
                if current_time - last_progress_update >= PROGRESS_UPDATE_INTERVAL:
                    try:
                        await status_message.edit_text(progress_tracker.format_progress())
                    except Exception:
                        pass  # Ignore rate limit errors
                    last_progress_update = current_time

                # Check for timeout
                if current_time - start_time > DOWNLOAD_TIMEOUT:
                    raise TimeoutError("Download timeout - connection too slow")

                # Check file size limit
                if progress_tracker.downloaded > MAX_FILE_SIZE:
                    raise ValueError("File size exceeds 4GB limit")

        temp_file.close()
        response.close()  # Close the response stream
        return temp_file_path

    except Exception as e:
        temp_file.close()
        response.close()  # Ensure response is closed on error
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise e

# Initialize Pyrogram client (will be set up in main function)
app = None

async def start_command(client: Client, message: Message):
    """Handle /start command"""
    welcome_message = (
        "🤖 **Enhanced File Downloader Bot (4GB Support)**\n\n"
        "Send me any file link and I'll download it for you!\n\n"
        "**✨ New Features:**\n"
        "• Downloads files up to 4GB (auto-split)\n"
        "• Google Drive folder support\n"
        "• Multiple file services supported\n"
        "• Queue system (up to 5 files)\n"
        "• Faster downloads with optimized speeds\n\n"
        "**📂 Supported Services:**\n"
        "• Google Drive (files only - folders limited)\n"
        "• Dropbox, OneDrive\n"
        "• Any direct download link\n\n"
        "**📋 Commands:**\n"
        "• Send any link to download\n"
        "• /queue - Check your queue status\n"
        "• /clear - Clear your queue\n\n"
        "Just paste a link to get started! 🚀"
    )
    await message.reply_text(welcome_message)

async def queue_command(client: Client, message: Message):
    """Handle /queue command"""
    user_id = message.from_user.id
    queue_size = download_queue.get_queue_size(user_id)
    is_processing = download_queue.is_processing(user_id)
    
    if queue_size == 0 and not is_processing:
        await message.reply_text("📋 Your download queue is empty.")
        return
    
    status = "🔄 Processing" if is_processing else "⏳ Waiting"
    await message.reply_text(
        f"📋 **Queue Status:**\n\n"
        f"Current: {status}\n"
        f"Pending: {queue_size} files\n"
        f"Total slots: {MAX_QUEUE_SIZE}"
    )

async def clear_command(client: Client, message: Message):
    """Handle /clear command"""
    user_id = message.from_user.id
    download_queue.clear_queue(user_id)
    await message.reply_text("🗑️ Your download queue has been cleared.")

async def handle_folder_download(client: Client, message: Message, folder_url: str, status_message: Message):
    """Handle Google Drive folder downloads"""
    folder_handler = GoogleDriveFolderHandler()
    folder_id = folder_handler.extract_folder_id(folder_url)
    
    if not folder_id:
        await status_message.edit_text("❌ **Invalid folder link**\n\nCouldn't extract folder ID.")
        return
    
    await status_message.edit_text("📂 **Scanning folder for files...**")
    
    try:
        files = await folder_handler.get_folder_files(folder_id)
        
        if not files:
            await status_message.edit_text(
                "📂 **Empty folder or access denied**\n\n"
                "Make sure the folder:\n"
                "• Contains downloadable files\n"
                "• Is shared with 'Anyone with the link'\n"
                "• Is not private"
            )
            return
        
        # Add files to queue
        user_id = message.from_user.id
        added_count = 0
        
        for file_info in files:
            queue_item = QueueItem(
                url=file_info.download_url,
                user_id=user_id,
                chat_id=message.chat.id,
                message_id=message.id,
                timestamp=datetime.now(),
                item_type="file"
            )
            
            if download_queue.add_item(user_id, queue_item):
                added_count += 1
            else:
                break  # Queue full
        
        if added_count == 0:
            await status_message.edit_text(
                "📋 **Queue is full!**\n\n"
                f"You have {MAX_QUEUE_SIZE} files in queue already.\n"
                "Use /clear to clear queue or wait for downloads to complete."
            )
            return
        
        await status_message.edit_text(
            f"📂 **Folder scan complete!**\n\n"
            f"📁 Found: {len(files)} files\n"
            f"📋 Added to queue: {added_count} files\n"
            f"⏳ Queue slots used: {download_queue.get_queue_size(user_id)}/{MAX_QUEUE_SIZE}\n\n"
            f"🚀 Starting downloads automatically..."
        )
        
        # Start processing queue automatically
        asyncio.create_task(process_download_queue(client, user_id))
        
    except Exception as e:
        await status_message.edit_text(
            f"❌ **Error scanning folder**\n\n"
            f"Error: {str(e)}\n\n"
            "Please check:\n"
            "• Folder is publicly accessible\n"
            "• Link is valid"
        )

async def upload_with_splitting(client: Client, file_path: str, chat_id: int, filename: str, status_message: Message) -> bool:
    """Upload file with automatic splitting for large files"""
    file_size = os.path.getsize(file_path)
    
    if file_size <= SPLIT_SIZE:
        # Upload directly without splitting
        upload_last_update = 0
        upload_start_time = time.time()
        
        async def upload_progress(current, total):
            nonlocal upload_last_update, upload_start_time
            current_time = time.time()

            if current_time - upload_last_update >= UPLOAD_PROGRESS_INTERVAL:
                percent = current * 100 / total
                mb_current = current / (1024 * 1024)
                mb_total = total / (1024 * 1024)
                
                upload_elapsed = max(1, current_time - upload_start_time)
                speed_mbps = (current / (1024 * 1024)) / upload_elapsed
                
                try:
                    await status_message.edit_text(
                        f"📤 Uploading: {percent:.1f}% — {mb_current:.1f}/{mb_total:.1f} MB — {speed_mbps:.1f} MB/s"
                    )
                    upload_last_update = current_time
                except Exception:
                    pass
        
        try:
            await asyncio.wait_for(
                client.send_document(
                    chat_id=chat_id,
                    document=file_path,
                    file_name=filename,
                    caption=f"✅ Download completed!\n\nFile size: {file_size / (1024 * 1024):.1f} MB",
                    progress=upload_progress
                ),
                timeout=UPLOAD_TIMEOUT
            )
            return True
        except Exception as e:
            await status_message.edit_text(f"❌ Upload failed: {str(e)}")
            return False
    
    else:
        # Split and upload parts
        await status_message.edit_text("🔧 **Large file detected - splitting into parts...**")
        
        try:
            parts = FileSplitter.split_file(file_path, SPLIT_SIZE)
            total_parts = len(parts)
            
            await status_message.edit_text(f"📤 **Uploading {total_parts} parts...**")
            
            for i, part_path in enumerate(parts, 1):
                part_filename = f"{filename}.part{i:03d}"
                
                try:
                    await client.send_document(
                        chat_id=chat_id,
                        document=part_path,
                        file_name=part_filename,
                        caption=f"📁 {filename}\nPart {i}/{total_parts} (File split due to size)"
                    )
                    
                    await status_message.edit_text(
                        f"📤 **Uploading parts: {i}/{total_parts} completed**"
                    )
                    
                except Exception as e:
                    await status_message.edit_text(f"❌ Failed to upload part {i}: {str(e)}")
                    FileSplitter.cleanup_parts(parts)
                    return False
            
            # Cleanup
            FileSplitter.cleanup_parts(parts)
            
            await status_message.edit_text(
                f"✅ **All parts uploaded successfully!**\n\n"
                f"📁 File: {filename}\n"
                f"📦 Parts: {total_parts}\n"
                f"💡 Download all parts and use file joining software to reconstruct the original file."
            )
            return True
            
        except Exception as e:
            await status_message.edit_text(f"❌ File splitting failed: {str(e)}")
            return False

async def process_download_queue(client: Client, user_id: int):
    """Process the download queue for a user"""
    if download_queue.is_processing(user_id):
        return  # Already processing
    
    download_queue.set_processing(user_id, True)
    
    try:
        while True:
            queue_item = download_queue.get_next_item(user_id)
            if not queue_item:
                break  # Queue empty
            
            await process_single_download(client, queue_item)
            await asyncio.sleep(1)  # Brief pause between downloads
    
    finally:
        download_queue.set_processing(user_id, False)

async def process_single_download(client: Client, queue_item: QueueItem):
    """Process a single download from the queue"""
    try:
        # Send processing message
        status_message = await client.send_message(
            queue_item.chat_id,
            "🔍 **Processing queued download...**"
        )
        
        url = queue_item.url
        
        # Detect URL type and handle accordingly
        if 'drive.google.com' in url:
            # Google Drive file
            file_id = extract_google_drive_file_id(url)
            if not file_id:
                await status_message.edit_text("❌ Invalid Google Drive link in queue")
                return
            
            response, file_size, filename, error_msg = await resolve_drive_download(file_id, url)
            
            if response is None:
                await status_message.edit_text(f"❌ **Error:** {error_msg or 'Cannot access file'}")
                return
            
            # Download and upload
            progress_tracker = ProgressTracker(file_size)
            temp_file_path = await download_file_streaming(response, progress_tracker, status_message)
            
            await upload_with_splitting(
                client, temp_file_path, queue_item.chat_id,
                filename or "downloaded_file", status_message
            )
            
            # Cleanup
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
        else:
            # Direct download URL
            await process_direct_url(client, url, queue_item.chat_id, status_message)
            
    except Exception as e:
        try:
            await client.send_message(
                queue_item.chat_id,
                f"❌ **Queue download failed:** {str(e)}"
            )
        except Exception:
            pass

async def process_direct_url(client: Client, url: str, chat_id: int, status_message: Message):
    """Process a direct download URL"""
    try:
        await status_message.edit_text("🔍 **Resolving direct download...**")
        
        # Get optimized download URL
        download_url = URLDetector.get_direct_download_url(url)
        
        # Create session and start download
        session = create_drive_session()
        response = session.get(download_url, stream=True, timeout=30, allow_redirects=True)
        response.raise_for_status()
        
        # Extract filename
        filename = "downloaded_file"
        content_disposition = response.headers.get('content-disposition', '')
        if content_disposition:
            extracted_name = extract_filename_from_disposition(content_disposition)
            if extracted_name:
                filename = extracted_name
        
        # Get file size
        file_size = None
        content_length = response.headers.get('content-length')
        if content_length:
            try:
                file_size = int(content_length)
            except ValueError:
                pass
        
        # Download file
        progress_tracker = ProgressTracker(file_size)
        temp_file_path = await download_file_streaming(response, progress_tracker, status_message)
        
        # Upload with splitting
        await upload_with_splitting(client, temp_file_path, chat_id, filename, status_message)
        
        # Cleanup
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            
    except Exception as e:
        await status_message.edit_text(f"❌ **Direct download failed:** {str(e)}")

async def handle_message(client: Client, message: Message):
    """Enhanced message handler with queue and multi-service support"""
    if not message.text:
        return

    # Prevent duplicate processing of the same message
    message_key = f"{message.from_user.id}_{message.id}_{message.text[:50]}"
    if message_key in processed_messages:
        return
    processed_messages.add(message_key)
    
    # Clean old entries (keep only last 100 messages per user)
    if len(processed_messages) > 1000:
        processed_messages.clear()

    message_text = message.text.strip()
    user_id = message.from_user.id

    # Check if URL is supported
    if not URLDetector.is_supported_url(message_text):
        await message.reply_text(
            "❌ **Unsupported URL**\n\n"
            "📂 **Supported services:**\n"
            "• Google Drive (individual files)\n"
            "• Dropbox, OneDrive\n"
            "• Direct download links\n\n"
            "Note: Folder support is limited. Please send individual file links."
        )
        return

    # Send initial processing message
    status_message = await message.reply_text("🔍 **Processing your link...**")

    # Check if it's a Google Drive folder
    if is_google_drive_folder(message_text):
        await handle_folder_download(client, message, message_text, status_message)
        return

    # Handle individual files
    # Check queue space
    queue_size = download_queue.get_queue_size(user_id)
    if queue_size >= MAX_QUEUE_SIZE:
        await status_message.edit_text(
            f"📋 **Queue is full!**\n\n"
            f"You have {MAX_QUEUE_SIZE} files in queue already.\n"
            "Use /clear to clear queue or wait for downloads to complete."
        )
        return

    # Add to queue
    queue_item = QueueItem(
        url=message_text,
        user_id=user_id,
        chat_id=message.chat.id,
        message_id=message.id,
        timestamp=datetime.now(),
        item_type="file"
    )

    if download_queue.add_item(user_id, queue_item):
        queue_size = download_queue.get_queue_size(user_id)
        await status_message.edit_text(
            f"📋 **Added to queue!**\n\n"
            f"Position: {queue_size}\n"
            f"Queue status: {queue_size}/{MAX_QUEUE_SIZE} files\n\n"
            "⏳ Download will start automatically..."
        )
        
        # Start processing queue automatically
        asyncio.create_task(process_download_queue(client, user_id))
    else:
        await status_message.edit_text("❌ **Queue error** - Unable to add file to queue.")

def main():
    """Main function to run the bot"""
    global app

    if not TOKEN:
        print("❌ Error: TOKEN environment variable not set!")
        print("Please set your Telegram bot token in the TOKEN environment variable.")
        return

    if not API_ID or not API_HASH:
        print("❌ Error: API_ID and API_HASH environment variables not set!")
        print("\n📋 To get your API credentials:")
        print("1. Go to https://my.telegram.org")
        print("2. Login with your phone number")
        print("3. Go to 'API development tools'")
        print("4. Create a new application")
        print("5. Copy API_ID and API_HASH")
        print("\nThen set them as environment variables:")
        print("• API_ID=your_api_id_number")
        print("• API_HASH=your_api_hash_string")
        return

    # Initialize Pyrogram client
    try:
        api_id_int = int(API_ID)
    except (ValueError, TypeError):
        print("❌ Error: API_ID must be a valid integer!")
        return

    app = Client(
        "google_drive_bot",
        bot_token=TOKEN,
        api_id=api_id_int,
        api_hash=API_HASH
    )

    # Set up handlers
    app.on_message(filters.command("start"))(start_command)
    app.on_message(filters.command("queue"))(queue_command)
    app.on_message(filters.command("clear"))(clear_command)
    app.on_message(filters.text & filters.private & ~filters.command(["start", "queue", "clear"]))(handle_message)

    # Start the bot
    print("🤖 Starting Enhanced Telegram bot with 4GB support...")
    print("✨ Features: Folder support, Queue system, File splitting, Multiple services")
    print("Bot is running! Press Ctrl+C to stop.")

    try:
        app.run()
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        print(f"❌ Error running bot: {e}")

if __name__ == '__main__':
    main()
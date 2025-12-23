#!/usr/bin/env python3
"""
Stage 2: The Factory - Video Rendering Pipeline
================================================

This script bridges n8n Stage 1 (The Hunter) and Stage 3 (The Broadcaster).

Process:
1. Read rows from Google Sheets where Status = 'Ready'
2. Claim row by setting Status = 'Processing'
3. Render video using your preferred method (MoviePy, FFmpeg, etc.)
4. Upload to Google Drive
5. Update row with drive_file_id and Status = 'Done'

Requirements:
    pip install gspread google-auth google-api-python-client moviepy

Setup:
    1. Create a Google Cloud project
    2. Enable Sheets API and Drive API
    3. Create a service account and download credentials.json
    4. Share your Google Sheet with the service account email

Usage:
    python stage2_factory.py

    Or run as a cron job:
    */15 * * * * /usr/bin/python3 /path/to/stage2_factory.py >> /var/log/factory.log 2>&1
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

# Google APIs
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ============================================================================
# CONFIGURATION
# ============================================================================

# Path to your Google service account credentials
CREDENTIALS_FILE = os.environ.get(
    'GOOGLE_CREDENTIALS_FILE',
    './credentials.json'
)

# Google Sheet ID (from the URL)
SHEET_ID = os.environ.get(
    'GOOGLE_SHEET_ID',
    'REPLACE_WITH_YOUR_SHEET_ID'
)

# Sheet tab name
SHEET_NAME = 'ETL_Pipeline'

# Google Drive folder ID for uploads
DRIVE_FOLDER_ID = os.environ.get(
    'GOOGLE_DRIVE_FOLDER_ID',
    'REPLACE_WITH_YOUR_FOLDER_ID'
)

# Temp directory for video rendering
TEMP_DIR = os.environ.get('TEMP_DIR', './temp')

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# GOOGLE API SETUP
# ============================================================================

def get_google_credentials():
    """Load Google service account credentials."""
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file'
    ]
    return Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=scopes
    )

def get_sheets_client():
    """Get authenticated Google Sheets client."""
    creds = get_google_credentials()
    return gspread.authorize(creds)

def get_drive_service():
    """Get authenticated Google Drive service."""
    creds = get_google_credentials()
    return build('drive', 'v3', credentials=creds)

# ============================================================================
# GOOGLE SHEETS OPERATIONS
# ============================================================================

def get_ready_rows(sheet) -> list:
    """
    Fetch all rows where Status = 'Ready'.
    
    Returns:
        List of dicts with row data and row number
    """
    all_records = sheet.get_all_records()
    ready_rows = []
    
    for idx, record in enumerate(all_records, start=2):  # Row 1 is header
        if record.get('Status', '').lower() == 'ready':
            record['_row_number'] = idx
            ready_rows.append(record)
    
    logger.info(f"Found {len(ready_rows)} rows with Status='Ready'")
    return ready_rows

def claim_row(sheet, row_number: int, record_id: str) -> bool:
    """
    Claim a row by setting Status to 'Processing'.
    
    This prevents race conditions if multiple instances run.
    """
    try:
        # Find Status column (assuming it's column J = index 10)
        status_col = sheet.find('Status').col
        
        # Update to 'Processing'
        sheet.update_cell(row_number, status_col, 'Processing')
        logger.info(f"Claimed row {row_number} (id={record_id})")
        return True
        
    except Exception as e:
        logger.error(f"Failed to claim row {row_number}: {e}")
        return False

def update_row_done(sheet, row_number: int, drive_file_id: str) -> bool:
    """
    Update row with drive_file_id and Status = 'Done'.
    """
    try:
        # Find column indices
        header_row = sheet.row_values(1)
        status_col = header_row.index('Status') + 1
        drive_col = header_row.index('drive_file_id') + 1
        
        # Batch update
        sheet.update_cell(row_number, status_col, 'Done')
        sheet.update_cell(row_number, drive_col, drive_file_id)
        
        logger.info(f"Row {row_number} marked as Done (drive_file_id={drive_file_id})")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update row {row_number}: {e}")
        return False

def update_row_error(sheet, row_number: int, error_message: str) -> bool:
    """
    Update row with error status and message.
    """
    try:
        header_row = sheet.row_values(1)
        status_col = header_row.index('Status') + 1
        error_col = header_row.index('error_message') + 1
        
        sheet.update_cell(row_number, status_col, 'Error')
        sheet.update_cell(row_number, error_col, error_message[:500])  # Truncate
        
        logger.info(f"Row {row_number} marked as Error")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update row {row_number} with error: {e}")
        return False

# ============================================================================
# VIDEO RENDERING
# ============================================================================

def render_video(record: Dict[str, Any], output_path: str) -> bool:
    """
    Render video from the record data.
    
    This is a placeholder - implement your actual rendering logic here.
    
    Options:
    - MoviePy: Python library for video editing
    - FFmpeg: Command-line video processing
    - Remotion: React-based video rendering
    - Custom API: Call your rendering service
    
    Args:
        record: Dict with script, image_prompt, title, etc.
        output_path: Where to save the rendered video
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Rendering video for: {record.get('title', 'Unknown')}")
    
    # ========================================================================
    # PLACEHOLDER: Replace with your actual video rendering logic
    # ========================================================================
    
    # Example using MoviePy (uncomment and modify as needed):
    """
    from moviepy.editor import (
        TextClip, ColorClip, CompositeVideoClip, 
        concatenate_videoclips, AudioFileClip
    )
    
    # Get data from record
    script = record.get('script', '')
    title = record.get('ai_title', record.get('title', 'News'))
    image_prompt = record.get('image_prompt', '')
    
    # Create video clips
    # 1. Generate background image from image_prompt (use DALL-E, Pollinations, etc.)
    # 2. Generate audio from script (use EdgeTTS, ElevenLabs, etc.)
    # 3. Combine into final video
    
    # Example: Simple text on color background
    duration = 30  # seconds
    
    background = ColorClip(size=(1920, 1080), color=(30, 30, 50), duration=duration)
    
    title_clip = TextClip(
        title,
        fontsize=60,
        color='white',
        font='Arial-Bold',
        size=(1800, None)
    ).set_position('center').set_duration(duration)
    
    final = CompositeVideoClip([background, title_clip])
    final.write_videofile(output_path, fps=24, codec='libx264')
    
    return True
    """
    
    # For now, create a placeholder file for testing
    import shutil
    
    # Check if you have a template video to copy
    template_video = './template_video.mp4'
    if os.path.exists(template_video):
        shutil.copy(template_video, output_path)
        logger.info(f"Copied template video to {output_path}")
        return True
    
    # Create empty file for testing (replace with real rendering)
    logger.warning("No template video found - creating placeholder")
    with open(output_path, 'wb') as f:
        f.write(b'PLACEHOLDER VIDEO FILE')
    
    return True

# ============================================================================
# GOOGLE DRIVE UPLOAD
# ============================================================================

def upload_to_drive(file_path: str, file_name: str) -> Optional[str]:
    """
    Upload a file to Google Drive.
    
    Args:
        file_path: Local path to the video file
        file_name: Name for the file in Drive
        
    Returns:
        Google Drive file ID if successful, None otherwise
    """
    try:
        service = get_drive_service()
        
        file_metadata = {
            'name': file_name,
            'parents': [DRIVE_FOLDER_ID] if DRIVE_FOLDER_ID else []
        }
        
        media = MediaFileUpload(
            file_path,
            mimetype='video/mp4',
            resumable=True
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        file_id = file.get('id')
        logger.info(f"Uploaded to Drive: {file_name} (id={file_id})")
        
        return file_id
        
    except Exception as e:
        logger.error(f"Failed to upload to Drive: {e}")
        return None

# ============================================================================
# MAIN PROCESSING LOOP
# ============================================================================

def process_row(sheet, record: Dict[str, Any]) -> bool:
    """
    Process a single row through the video pipeline.
    
    Args:
        sheet: Google Sheet worksheet object
        record: Row data with _row_number
        
    Returns:
        True if successful, False otherwise
    """
    row_number = record['_row_number']
    record_id = record.get('id', f'row-{row_number}')
    title = record.get('title', 'Unknown')
    
    logger.info(f"Processing: {title}")
    
    # Step 1: Claim the row
    if not claim_row(sheet, row_number, record_id):
        return False
    
    try:
        # Step 2: Prepare output path
        os.makedirs(TEMP_DIR, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_title = ''.join(c for c in title[:30] if c.isalnum() or c in ' -_')
        output_filename = f"{timestamp}_{safe_title}.mp4"
        output_path = os.path.join(TEMP_DIR, output_filename)
        
        # Step 3: Render video
        if not render_video(record, output_path):
            raise Exception("Video rendering failed")
        
        # Step 4: Upload to Google Drive
        drive_file_id = upload_to_drive(output_path, output_filename)
        if not drive_file_id:
            raise Exception("Drive upload failed")
        
        # Step 5: Update sheet with success
        update_row_done(sheet, row_number, drive_file_id)
        
        # Step 6: Cleanup temp file
        if os.path.exists(output_path):
            os.remove(output_path)
            logger.info(f"Cleaned up temp file: {output_path}")
        
        logger.info(f"✅ Successfully processed: {title}")
        return True
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ Failed to process {title}: {error_msg}")
        update_row_error(sheet, row_number, error_msg)
        return False

def main():
    """
    Main entry point for the Factory script.
    """
    logger.info("=" * 60)
    logger.info("Stage 2: The Factory - Starting")
    logger.info("=" * 60)
    
    try:
        # Connect to Google Sheets
        client = get_sheets_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(SHEET_NAME)
        
        # Get ready rows
        ready_rows = get_ready_rows(sheet)
        
        if not ready_rows:
            logger.info("No rows to process. Exiting.")
            return
        
        # Process each row
        success_count = 0
        error_count = 0
        
        for record in ready_rows:
            if process_row(sheet, record):
                success_count += 1
            else:
                error_count += 1
        
        # Summary
        logger.info("=" * 60)
        logger.info(f"Processing complete: {success_count} success, {error_count} errors")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == '__main__':
    main()

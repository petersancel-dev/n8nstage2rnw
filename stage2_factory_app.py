"""
Stage 2: The Factory - Flask Web App
Deploy to Render.com (free tier)
"""
import os
import json
import logging
import threading
from datetime import datetime
from flask import Flask, request, jsonify
# Google APIs
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import requests
# =============================================================================
# FLASK APP - This line is CRITICAL for Gunicorn
# =============================================================================
app = Flask(__name__)
# =============================================================================
# CONFIGURATION
# =============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)
SHEET_ID = os.environ.get('GOOGLE_SHEET_ID', '')
SHEET_NAME = 'ETL_Pipeline'
DRIVE_FOLDER_ID = os.environ.get('GOOGLE_DRIVE_FOLDER', '')
TEMP_DIR = '/tmp'
# =============================================================================
# GOOGLE API SETUP
# =============================================================================
def get_google_credentials():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS', '')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS not set")
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']
    return Credentials.from_service_account_info(creds_dict, scopes=scopes)
def get_sheets_client():
    return gspread.authorize(get_google_credentials())
def get_drive_service():
    return build('drive', 'v3', credentials=get_google_credentials())
# =============================================================================
# GOOGLE SHEETS OPERATIONS
# =============================================================================
def find_row_by_id(sheet, record_id):
    try:
        cell = sheet.find(record_id)
        return cell.row if cell else None
    except:
        return None
def update_row_status(sheet, row_number, status, drive_file_id=None, error_msg=None):
    header_row = sheet.row_values(1)
    status_col = header_row.index('Status') + 1
    sheet.update_cell(row_number, status_col, status)
    
    if drive_file_id and 'drive_file_id' in header_row:
        drive_col = header_row.index('drive_file_id') + 1
        sheet.update_cell(row_number, drive_col, drive_file_id)
    
    if error_msg and 'error_message' in header_row:
        error_col = header_row.index('error_message') + 1
        sheet.update_cell(row_number, error_col, error_msg[:500])
# =============================================================================
# VIDEO RENDERING (PLACEHOLDER)
# =============================================================================
def render_video(record, output_path):
    """Replace this with your actual video rendering logic."""
    logger.info(f"Rendering video for: {record.get('title', 'Unknown')}")
    try:
        # Placeholder: download a sample video
        response = requests.get("https://www.w3schools.com/html/mov_bbb.mp4", timeout=30)
        with open(output_path, 'wb') as f:
            f.write(response.content)
        return True
    except Exception as e:
        logger.error(f"Render failed: {e}")
        return False
# =============================================================================
# GOOGLE DRIVE UPLOAD
# =============================================================================
def upload_to_drive(file_path, file_name):
    try:
        service = get_drive_service()
        file_metadata = {'name': file_name}
        if DRIVE_FOLDER_ID:
            file_metadata['parents'] = [DRIVE_FOLDER_ID]
        
        media = MediaFileUpload(file_path, mimetype='video/mp4', resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return None
# =============================================================================
# PROCESSING LOGIC
# =============================================================================
def process_record(record_id):
    logger.info(f"Processing: {record_id}")
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        
        row_number = find_row_by_id(sheet, record_id)
        if not row_number:
            logger.error(f"Record not found: {record_id}")
            return
        
        header_row = sheet.row_values(1)
        row_values = sheet.row_values(row_number)
        record = dict(zip(header_row, row_values))
        
        update_row_status(sheet, row_number, 'Processing')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        title = record.get('title', 'video')[:20]
        safe_title = ''.join(c for c in title if c.isalnum() or c in ' -_')
        output_filename = f"{timestamp}_{safe_title}.mp4"
        output_path = os.path.join(TEMP_DIR, output_filename)
        
        if not render_video(record, output_path):
            raise Exception("Video rendering failed")
        
        drive_file_id = upload_to_drive(output_path, output_filename)
        if not drive_file_id:
            raise Exception("Drive upload failed")
        
        update_row_status(sheet, row_number, 'Done', drive_file_id=drive_file_id)
        
        if os.path.exists(output_path):
            os.remove(output_path)
        
        logger.info(f"✅ Done: {record_id}")
        
    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        try:
            update_row_status(sheet, row_number, 'Error', error_msg=str(e))
        except:
            pass
# =============================================================================
# FLASK ROUTES
# =============================================================================
@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'Stage 2: The Factory'})
@app.route('/process', methods=['POST'])
def process():
    data = request.get_json() or {}
    record_id = data.get('record_id')
    
    if not record_id:
        return jsonify({'error': 'record_id required'}), 400
    if not SHEET_ID:
        return jsonify({'error': 'GOOGLE_SHEET_ID not set'}), 500
    
    thread = threading.Thread(target=process_record, args=(record_id,))
    thread.start()
    
    return jsonify({'status': 'accepted', 'record_id': record_id}), 202
@app.route('/process-all', methods=['POST'])
def process_all():
    if not SHEET_ID:
        return jsonify({'error': 'GOOGLE_SHEET_ID not set'}), 500
    
    try:
        client = get_sheets_client()
        sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        records = sheet.get_all_records()
        ready_ids = [r['id'] for r in records if r.get('Status', '').lower() == 'ready' and r.get('id')]
        
        for rid in ready_ids:
            thread = threading.Thread(target=process_record, args=(rid,))
            thread.start()
        
        return jsonify({'status': 'accepted', 'count': len(ready_ids)}), 202
    except Exception as e:
        return jsonify({'error': str(e)}), 500
# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

import os
import io
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from dotenv import load_dotenv

load_dotenv()

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVICE_ACCOUNT_FILE = os.path.join(base_dir, "service_account.json")
FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")  # ✅ Consistent with .env
SCOPES = ['https://www.googleapis.com/auth/drive']  # ✅ Full access scope

def upload_to_drive(filepath, folder_id=FOLDER_ID):
    filename = os.path.basename(filepath)

    if not folder_id:
        print("Google Drive folder ID not found. Check your .env file for DRIVE_FOLDER_ID.")
        return

    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)
    except Exception as e:
        print(f"Failed to authenticate Google Drive service: {e}")
        return

    # Step 1: Check if file already exists in Drive
    try:
        query = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
        results = service.files().list(
            q=query,
            spaces='drive',
            fields="files(id, name)",
            supportsAllDrives=True
        ).execute()

        items = results.get('files', [])
    except Exception as e:
        print(f"Error querying Drive: {e}")
        return

    file_id = items[0]['id'] if items else None

    # Step 2: Try downloading existing file if it exists
    df_existing = None
    if file_id:
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            df_existing = pd.read_csv(fh, parse_dates=["Date"])
        except Exception:
            print(f"⚠️ Could not read or parse existing file. Will overwrite instead.")
            df_existing = None

    # Step 3: Read local file
    try:
        df_new = pd.read_csv(filepath, parse_dates=["Date"])
    except Exception as e:
        print(f"Failed to read local file {filepath}: {e}")
        return

    # Step 4: Merge data
    if df_existing is not None:
        combined = pd.concat([df_existing, df_new], ignore_index=True)
        combined.drop_duplicates(subset=["Date"], inplace=True)
    else:
        combined = df_new

    # Step 5: Save merged file locally as temp and upload to Drive
    temp_path = filepath + ".tmp"
    combined.to_csv(temp_path, index=False)
    with open(temp_path, "rb") as f:
        media = MediaFileUpload(temp_path, resumable=True)

        if file_id:
            service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True
            ).execute()
            print(f"Updated {filename} on Shared Drive")
        else:
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            print(f"Uploaded new file: {filename} to Shared Drive")

    # ✅ File is now closed — safe to delete
    os.remove(temp_path)

    try:
        # Update the file creation and update methods
        if file_id:
            service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True  # Add this
            ).execute()
        else:
            file_metadata = {
                'name': filename,
                'parents': [folder_id],
                # Add driveId to specify shared drive
                'driveId': folder_id.split('_')[0] if '_' in folder_id else folder_id
            }
            service.files().create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True,  # Ensure this is True
                fields='id'
            ).execute()

            print(f"Uploaded new file: {filename} to Google Drive.")
    except Exception as e:
        print(f"Failed to upload to Google Drive: {e}")
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

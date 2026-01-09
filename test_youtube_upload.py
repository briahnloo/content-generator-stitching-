#!/usr/bin/env python3
"""
Test script for YouTube upload functionality.
Uploads a single video file directly to YouTube.
"""

import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_upload(video_path: str, title: str = "Test Upload - Viral Clips", privacy: str = "private"):
    """Upload a video file directly to YouTube for testing."""

    video_file = Path(video_path)
    if not video_file.exists():
        print(f"Error: Video file not found: {video_path}")
        return None

    # Check credentials
    if not settings.YOUTUBE_CLIENT_ID:
        print("Error: YOUTUBE_CLIENT_ID not set in .env")
        return None
    if not settings.YOUTUBE_CLIENT_SECRET:
        print("Error: YOUTUBE_CLIENT_SECRET not set in .env")
        return None
    if not settings.YOUTUBE_REFRESH_TOKEN:
        print("Error: YOUTUBE_REFRESH_TOKEN not set in .env")
        return None

    print(f"Credentials found:")
    print(f"  CLIENT_ID: {settings.YOUTUBE_CLIENT_ID[:20]}...")
    print(f"  CLIENT_SECRET: {settings.YOUTUBE_CLIENT_SECRET[:10]}...")
    print(f"  REFRESH_TOKEN: {settings.YOUTUBE_REFRESH_TOKEN[:20]}...")

    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        print("\nBuilding YouTube service...")

        credentials = Credentials(
            None,
            refresh_token=settings.YOUTUBE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=settings.YOUTUBE_CLIENT_ID,
            client_secret=settings.YOUTUBE_CLIENT_SECRET,
        )

        youtube = build("youtube", "v3", credentials=credentials)

        print("YouTube service built successfully!")

        # Build metadata
        metadata = {
            "snippet": {
                "title": title[:100],
                "description": "Test upload from Viral Clips Pipeline\n\n#shorts #viral #test",
                "tags": ["shorts", "viral", "test", "compilation"],
                "categoryId": "24",  # Entertainment
            },
            "status": {
                "privacyStatus": privacy,
                "selfDeclaredMadeForKids": False,
            },
        }

        print(f"\nUploading: {video_file.name}")
        print(f"Title: {title}")
        print(f"Privacy: {privacy}")

        media = MediaFileUpload(
            str(video_file),
            chunksize=1024 * 1024,  # 1MB chunks
            resumable=True,
            mimetype="video/mp4",
        )

        request = youtube.videos().insert(
            part="snippet,status",
            body=metadata,
            media_body=media,
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"  Upload progress: {int(status.progress() * 100)}%")

        video_id = response["id"]
        print(f"\n{'='*60}")
        print("UPLOAD SUCCESSFUL!")
        print(f"{'='*60}")
        print(f"Video ID: {video_id}")
        print(f"URL: https://youtube.com/watch?v={video_id}")
        print(f"Shorts URL: https://youtube.com/shorts/{video_id}")
        print(f"{'='*60}")

        return video_id

    except ImportError as e:
        print(f"\nError: Missing dependencies. Install with:")
        print("  pip install google-api-python-client google-auth-oauthlib")
        return None
    except Exception as e:
        print(f"\nUpload failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Test with the specified video
    video_path = "/Users/bzliu/Desktop/EXTRANEOUS_CODE/content generation (stitching)/viral-clips-pipeline/output/review/057584c0-dbc.mp4"

    result = test_upload(
        video_path=video_path,
        title="Epic Fails Compilation #shorts",
        privacy="private"  # Start as private for safety
    )

    if result:
        print(f"\nTest passed! Video uploaded with ID: {result}")
    else:
        print("\nTest failed. Check the errors above.")

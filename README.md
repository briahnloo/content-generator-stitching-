# Viral Clips Pipeline

Automated pipeline that discovers trending TikTok videos, categorizes them, and stitches similar clips into compilation videos with text overlays and background music.

## Pipeline Flow

```
Apify (TikTok) → Download (yt-dlp) → Classify (GPT-4o-mini) → Group → Caption → Stitch (FFmpeg) → Review → Upload
```

## Quick Start

### 1. Install Dependencies

```bash
# Python dependencies
pip install -r requirements.txt

# System dependencies (macOS)
brew install ffmpeg

# System dependencies (Ubuntu)
sudo apt install ffmpeg
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your API keys
```

Required API keys:
- `APIFY_API_TOKEN` - Get from [Apify Console](https://console.apify.com/account/integrations)
- `OPENAI_API_KEY` - Get from [OpenAI Platform](https://platform.openai.com/api-keys)

### 3. Add Background Music

Drop royalty-free `.mp3` files into `config/music/`.

Free sources:
- [YouTube Audio Library](https://studio.youtube.com/channel/UC/music)
- [Pixabay Music](https://pixabay.com/music/)

### 4. Run the Pipeline

```bash
# Full pipeline: discover → classify → group → stitch
python cli.py run --discover-limit 50 --compilations 3

# Review generated videos in output/review/
python cli.py review

# Approve and upload
python cli.py approve <compilation_id>
python cli.py upload <compilation_id>
```

## CLI Commands

### Discovery & Download

```bash
# Discover trending videos
python cli.py discover --limit 50

# Discover from specific hashtag
python cli.py discover --hashtag fails --limit 30
```

### Classification

```bash
# Classify downloaded videos
python cli.py classify

# Classify with limit
python cli.py classify --limit 20
```

### Grouping & Stitching

```bash
# Create compilations
python cli.py group --compilations 3

# Render compilations
python cli.py stitch

# Render specific compilation
python cli.py stitch --id <compilation_id>
```

### Review & Upload

```bash
# List compilations ready for review
python cli.py review

# Approve compilation
python cli.py approve <compilation_id>

# Reject compilation
python cli.py reject <compilation_id>

# Upload to YouTube
python cli.py upload <compilation_id>
```

### Utilities

```bash
# Show pipeline status
python cli.py status

# List all compilations
python cli.py list-compilations

# YouTube OAuth setup
python cli.py auth

# Reset database
python cli.py reset --confirm
```

## Categories

The pipeline classifies videos into 8 categories:

| Category | Description |
|----------|-------------|
| fails | Accidents, mishaps, things going wrong |
| satisfying | Oddly satisfying, ASMR content |
| wholesome | Heartwarming, feel-good moments |
| comedy | Funny skits, humor, pranks |
| skills | Impressive talents, pro-level performance |
| animals | Pets, wildlife, cute animals |
| food | Cooking, eating, recipes |
| drama | Confrontations, public freakouts |

## Project Structure

```
viral-clips-pipeline/
├── config/
│   ├── settings.py           # Configuration management
│   ├── categories.yaml       # Category definitions
│   └── music/                # Background music files
├── core/
│   ├── models.py             # Data models
│   └── database.py           # SQLite CRUD
├── services/
│   ├── discovery.py          # Apify TikTok scraper
│   ├── downloader.py         # yt-dlp wrapper
│   ├── classifier.py         # GPT-4o-mini categorization
│   ├── grouper.py            # Compilation creation
│   ├── captioner.py          # Caption generation
│   ├── stitcher.py           # FFmpeg rendering
│   └── uploader.py           # YouTube API
├── pipeline.py               # Orchestrator
├── cli.py                    # CLI interface
├── requirements.txt
└── .env.example
```

## Cost Estimates

| Component | Cost per Run (50 videos, 3 compilations) |
|-----------|------------------------------------------|
| Apify | ~$0.50-1.00 |
| OpenAI (classify) | ~$0.05 |
| OpenAI (caption) | ~$0.01 |
| **Total** | **~$0.56-1.06** |

Monthly estimate (daily runs): $17-32/month

## YouTube Upload Setup (Optional)

1. Create a project in [Google Cloud Console](https://console.cloud.google.com)
2. Enable YouTube Data API v3
3. Create OAuth 2.0 credentials
4. Add credentials to `.env`:
   ```
   YOUTUBE_CLIENT_ID=your_client_id
   YOUTUBE_CLIENT_SECRET=your_client_secret
   ```
5. Run OAuth flow:
   ```bash
   python cli.py auth
   ```
6. Add the refresh token to `.env`:
   ```
   YOUTUBE_REFRESH_TOKEN=your_refresh_token
   ```

## Configuration

Key settings in `.env`:

```bash
# Compilation settings
MIN_CLIPS_PER_COMPILATION=5
MAX_CLIPS_PER_COMPILATION=8
MIN_CLASSIFICATION_CONFIDENCE=0.3

# Video output
VIDEO_WIDTH=1080
VIDEO_HEIGHT=1920
MAX_CLIP_DURATION=15

# Discovery
DISCOVERY_HASHTAGS=viral,fyp,trending,fails,satisfying
```

## License

MIT

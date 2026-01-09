# Viral Clips Pipeline

Fully automated pipeline that discovers trending TikTok videos, categorizes them using AI, and stitches similar clips into compilation videos for publishing to YouTube and TikTok. Supports multiple accounts per platform with intelligent content routing.

## What It Does

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│  Discovery  │───▶│   Download   │───▶│  Classify   │───▶│    Group    │
│   (Apify)   │    │   (yt-dlp)   │    │ (GPT-4o-mini)│    │ (by category)│
└─────────────┘    └──────────────┘    └─────────────┘    └─────────────┘
                                                                  │
                                                                  ▼
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│   Upload    │◀───│    Route     │◀───│  Auto-Approve│◀───│   Render    │
│ (YT/TikTok) │    │ (to accounts)│    │ (confidence) │    │  (FFmpeg)   │
└─────────────┘    └──────────────┘    └─────────────┘    └─────────────┘
```

**End-to-end flow:**

1. **Discover** - Scrapes trending TikTok videos via Apify API
2. **Download** - Downloads videos using yt-dlp with metadata
3. **Classify** - AI categorizes content (fails, comedy) and filters unwanted content
4. **Group** - Clusters similar videos into 5-clip compilations
5. **Render** - Stitches clips with FFmpeg, adds background music
6. **Auto-Approve** - High-confidence compilations skip manual review
7. **Route** - Matches content to appropriate platform accounts
8. **Upload** - Publishes to YouTube and/or TikTok automatically

---

## Features

- **Multi-Account Support** - Manage 4-10+ accounts per platform
- **Multi-Platform** - YouTube and TikTok upload support
- **AI Classification** - GPT-4o-mini filters and categorizes content
- **Content Filtering** - Blacklists dancing, ads, thirst traps automatically
- **Auto-Approval** - High-confidence content skips manual review
- **Intelligent Routing** - Routes content to accounts by strategy
- **Rate Limiting** - Respects daily upload limits per account
- **Encrypted Credentials** - Secure storage with Fernet encryption
- **Background Daemon** - Runs autonomously with configurable schedule
- **Full CLI** - Complete command-line interface for all operations

---

## Quick Start

### 1. Install Dependencies

```bash
# Clone and enter directory
cd viral-clips-pipeline

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Install FFmpeg (macOS)
brew install ffmpeg

# Install FFmpeg (Ubuntu/Debian)
sudo apt install ffmpeg
```

### 2. Configure Environment

```bash
# Copy example config
cp .env.example .env

# Edit with your API keys
nano .env  # or use any text editor
```

**Required API keys:**

| Key | Source | Purpose |
|-----|--------|---------|
| `APIFY_API_TOKEN` | [Apify Console](https://console.apify.com/account/integrations) | TikTok scraping |
| `OPENAI_API_KEY` | [OpenAI Platform](https://platform.openai.com/api-keys) | Video classification |

### 3. Add Background Music

Drop royalty-free `.mp3` files into `config/music/`:

```bash
mkdir -p config/music
# Add your music files here
```

Free music sources:
- [YouTube Audio Library](https://studio.youtube.com/channel/UC/music)
- [Pixabay Music](https://pixabay.com/music/)
- [Uppbeat](https://uppbeat.io/)

### 4. Run Your First Pipeline

```bash
# Run the full pipeline once
python cli.py run --discover-limit 50 --compilations 2

# Check status
python cli.py status

# Review generated videos
python cli.py review

# Videos are in output/review/
ls output/review/
```

---

## Multi-Account Setup

### Adding YouTube Accounts

```bash
# 1. Create account in the system
python cli.py account add \
    --platform youtube \
    --name "Epic Fails Channel" \
    --strategy fails \
    --daily-limit 3

# 2. Note the account ID from output (e.g., "abc123def456")

# 3. Run OAuth flow to authenticate
python cli.py account auth abc123def456
# Follow prompts for Client ID, Client Secret
# Browser will open for Google sign-in

# 4. Verify credentials are stored
python cli.py account list
```

**Getting YouTube OAuth Credentials:**

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (or select existing)
3. Enable **YouTube Data API v3**
4. Go to **APIs & Services > Credentials**
5. Click **Create Credentials > OAuth 2.0 Client IDs**
6. Select **Desktop App**
7. Download and note Client ID + Client Secret

### Adding TikTok Accounts

```bash
# 1. Create account in the system
python cli.py account add \
    --platform tiktok \
    --name "Comedy TikTok" \
    --strategy comedy \
    --daily-limit 5

# 2. Make sure you're logged into TikTok in Chrome

# 3. Extract cookies from browser
python cli.py account set-cookies abc123def456 --browser chrome

# 4. Verify
python cli.py account list
```

### Setting Up Content Routing

Route specific content categories to specific accounts:

```bash
# Route "fails" content to your fails channel with high priority
python cli.py route add abc123def456 --category fails --min-confidence 0.7 --priority 2

# Route "comedy" content to your comedy channel
python cli.py route add xyz789ghi012 --category comedy --min-confidence 0.7 --priority 1

# View all routing rules
python cli.py route list
```

---

## Running Automatically (Daemon Mode)

The daemon runs all pipeline stages on a schedule:

```bash
# Start the daemon (runs in foreground)
python daemon.py

# Or with aggressive schedule (faster, more uploads)
python daemon.py --aggressive

# Run once and exit (useful for cron)
python daemon.py --run-now

# Check status
python daemon.py --status
```

**Default Schedule:**

| Job | Interval | Description |
|-----|----------|-------------|
| Discovery | Every 4 hours | Find new trending content |
| Download | Every 30 min | Download discovered videos |
| Classify | Every 30 min | AI categorization |
| Group | Every 1 hour | Create compilations |
| Render | Every 1 hour | FFmpeg rendering |
| Route | Every 30 min | Assign to accounts |
| Upload | Every 6 hours | Publish to platforms (4 times per day) |
| Reset Limits | Daily at midnight | Reset upload counters |

**Running in Background:**

```bash
# Using screen
screen -S pipeline
python daemon.py
# Ctrl+A, D to detach

# Using nohup
nohup python daemon.py > pipeline.log 2>&1 &

# Using tmux
tmux new -s pipeline
python daemon.py
# Ctrl+B, D to detach
```

---

## CLI Reference

### Pipeline Commands

```bash
# Full pipeline run
python cli.py run --discover-limit 50 --compilations 3

# Individual stages
python cli.py discover --limit 50      # Find videos
python cli.py classify                  # Categorize videos
python cli.py group --compilations 3    # Create compilations
python cli.py stitch                    # Render videos

# Review workflow
python cli.py review                    # List ready for review
python cli.py approve <id>              # Approve compilation
python cli.py reject <id>               # Reject compilation
python cli.py upload <id>               # Manual upload
```

### Account Management

```bash
# List accounts
python cli.py account list
python cli.py account list --platform youtube
python cli.py account list --all  # Include inactive

# Add accounts
python cli.py account add -p youtube -n "Channel Name" -s fails
python cli.py account add -p tiktok -n "TikTok Account" -s comedy

# Authentication
python cli.py account auth <id>           # YouTube OAuth
python cli.py account set-cookies <id>    # TikTok cookies

# Manage accounts
python cli.py account activate <id>
python cli.py account deactivate <id>
python cli.py account delete <id> --confirm
```

### Routing Rules

```bash
# Add routing rule
python cli.py route add <account_id> -c fails -m 0.7 -p 1

# List all rules
python cli.py route list

# Delete rule
python cli.py route delete <rule_id>
```

### Upload Queue

```bash
# View queue
python cli.py queue list
python cli.py queue list --platform youtube

# Queue statistics
python cli.py queue stats

# Retry failed uploads
python cli.py queue retry
```

### Daemon Control

```bash
# Start daemon
python cli.py daemon start
python cli.py daemon start --aggressive

# Run full pipeline once
python cli.py daemon run-now

# Status
python cli.py daemon status
```

### Utilities

```bash
# Pipeline status
python cli.py status

# List all compilations
python cli.py list-compilations

# Reset database (destructive!)
python cli.py reset --confirm
```

---

## Content Categories

The pipeline focuses on two categories for maximum viral potential:

| Category | Keywords | Content Type |
|----------|----------|--------------|
| **fails** | fail, accident, gone wrong, instant regret | Mishaps, accidents, things going wrong |
| **comedy** | funny, humor, prank, joke, meme | Skits, pranks, funny moments |

**Automatically Rejected:**
- Dancing/choreography
- Advertisements/sponsored content
- Thirst traps/modeling
- Beauty/makeup tutorials
- Lip syncing/music videos
- Lifestyle vlogs

---

## Project Structure

```
viral-clips-pipeline/
├── config/
│   ├── settings.py           # Configuration management
│   ├── categories.yaml       # Category definitions
│   └── music/                # Background music (.mp3)
├── core/
│   ├── models.py             # Data models (Video, Compilation, Account, etc.)
│   ├── database.py           # SQLite CRUD operations
│   └── encryption.py         # Fernet credential encryption
├── services/
│   ├── discovery.py          # Apify TikTok scraper
│   ├── downloader.py         # yt-dlp wrapper
│   ├── classifier.py         # GPT-4o-mini categorization
│   ├── grouper.py            # Compilation creation + auto-approval
│   ├── captioner.py          # Description generation
│   ├── stitcher.py           # FFmpeg video rendering
│   ├── account_manager.py    # Account CRUD + credentials
│   ├── upload_router.py      # Content routing logic
│   ├── youtube_uploader.py   # YouTube Data API v3
│   └── tiktok_uploader.py    # TikTok upload via cookies
├── scheduler/
│   └── jobs.py               # APScheduler job definitions
├── data/
│   ├── downloads/            # Downloaded TikTok videos
│   └── pipeline.db           # SQLite database
├── output/
│   └── review/               # Rendered compilations
├── daemon.py                 # Background scheduler runner
├── pipeline.py               # Pipeline orchestrator
├── cli.py                    # CLI interface
├── requirements.txt          # Python dependencies
├── .env.example              # Environment template
└── README.md                 # This file
```

---

## Configuration Reference

Key settings in `.env`:

```bash
# =============================================================================
# API KEYS (Required)
# =============================================================================
APIFY_API_TOKEN=apify_api_...
OPENAI_API_KEY=sk-...

# =============================================================================
# COMPILATION SETTINGS
# =============================================================================
MIN_CLIPS_PER_COMPILATION=5    # Clips per compilation
MAX_CLIPS_PER_COMPILATION=5    # Keep at 5 for ~60-75s total
MIN_CLASSIFICATION_CONFIDENCE=0.3

# =============================================================================
# VIDEO OUTPUT
# =============================================================================
VIDEO_WIDTH=1080               # 9:16 vertical for Shorts
VIDEO_HEIGHT=1920
MAX_CLIP_DURATION=15           # Max seconds per clip

# =============================================================================
# AUTO-APPROVAL (New in v2.0)
# =============================================================================
AUTO_APPROVE_THRESHOLD=0.75    # Auto-approve if confidence >= this
MIN_CONFIDENCE_FOR_UPLOAD=0.6  # Minimum to route to any account

# =============================================================================
# RATE LIMITS
# =============================================================================
YOUTUBE_DAILY_LIMIT_PER_ACCOUNT=3
TIKTOK_DAILY_LIMIT_PER_ACCOUNT=5

# =============================================================================
# ENCRYPTION
# =============================================================================
# Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
CREDENTIALS_ENCRYPTION_KEY=your-key-here
```

---

## Cost Estimates

| Component | Cost per Run (50 videos, 2 compilations) |
|-----------|------------------------------------------|
| Apify (TikTok scraping) | ~$0.50-1.00 |
| OpenAI (classification) | ~$0.05 |
| YouTube API | Free (quota limits apply) |
| TikTok API | Free (uses cookies) |
| **Total per run** | **~$0.55-1.05** |

**Monthly estimates (4 runs/day):**
- Conservative: ~$70/month
- With high volume: ~$130/month

---

## Troubleshooting

### "No credentials found for account"
```bash
# Re-run authentication
python cli.py account auth <account_id>
```

### "TikTok cookies expired"
```bash
# Log into TikTok in browser, then:
python cli.py account set-cookies <account_id> --browser chrome
```

### "YouTube quota exceeded"
- Wait 24 hours for quota reset
- Reduce `YOUTUBE_DAILY_LIMIT_PER_ACCOUNT`
- Add more YouTube accounts to distribute load

### "No videos classified"
- Check `OPENAI_API_KEY` is valid
- Lower `MIN_CLASSIFICATION_CONFIDENCE`
- Review logs: content may be getting rejected

### "FFmpeg not found"
```bash
# macOS
brew install ffmpeg

# Ubuntu
sudo apt install ffmpeg

# Verify
ffmpeg -version
```

---

## License

MIT License - Use freely for personal and commercial projects.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## Disclaimer

This tool is for educational purposes. Ensure you have rights to use any content you upload. Respect platform Terms of Service and copyright laws.

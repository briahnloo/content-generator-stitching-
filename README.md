# Viral Clips Pipeline

Fully automated pipeline for creating viral short-form video content. Supports TikTok compilation videos and Reddit story narration videos for publishing to YouTube and TikTok. Features multi-account support with intelligent content routing.

## What It Does

The pipeline supports **three modes** for creating viral content:

### Mode 1: Individual Clips Pipeline
Discovers individual viral clips and combines them into compilations.

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

### Mode 2: Source Compilations Pipeline (Mega-Compilations)
Finds existing TikTok compilations and stitches them into longer mega-compilations.

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│  Discover       │───▶│   Download   │───▶│    Group    │───▶│   Render    │
│  Compilations   │    │   (yt-dlp)   │    │  (by type)  │    │  (FFmpeg)   │
└─────────────────┘    └──────────────┘    └─────────────┘    └─────────────┘
```

**Individual Clips Flow:**

1. **Discover** - Scrapes trending TikTok videos via Apify API
2. **Download** - Downloads videos using yt-dlp with metadata
3. **Classify** - AI categorizes content (fails, comedy) and filters unwanted content
4. **Group** - Clusters similar videos into 5-clip compilations
5. **Render** - Stitches clips with FFmpeg, adds background music
6. **Auto-Approve** - High-confidence compilations skip manual review
7. **Route** - Matches content to appropriate platform accounts
8. **Upload** - Publishes to YouTube and/or TikTok automatically

**Source Compilations Flow:**

1. **Discover Compilations** - Finds existing TikTok compilation videos (top 10s, fail compilations, etc.)
2. **Download** - Downloads the full compilation videos
3. **Group** - Combines multiple source compilations into mega-compilations
4. **Render** - Stitches source compilations together with transitions

### Mode 3: Reddit Story Narration Pipeline
Scrapes text stories from Reddit, generates TTS narration, and creates videos with synchronized captions over gameplay footage.

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│  Discover       │───▶│  Generate    │───▶│   Compose   │───▶│   Review    │
│  (PRAW/Reddit)  │    │  (Edge TTS)  │    │  (FFmpeg)   │    │  & Upload   │
└─────────────────┘    └──────────────┘    └─────────────┘    └─────────────┘
```

**Reddit Story Flow:**

1. **Discover** - Scrapes top posts from r/AITA, r/tifu, r/pettyrevenge, etc. via PRAW
2. **Generate** - Converts text to speech using Edge TTS with word-level timings
3. **Compose** - Overlays audio + synced captions on background gameplay footage
4. **Review** - Manual approval before upload to platforms

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
- **Reddit Stories** - Scrape and narrate Reddit posts with TTS
- **Synced Captions** - Word-level caption synchronization for narration videos
- **Free TTS** - High-quality Edge TTS (no API costs)

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
| `REDDIT_CLIENT_ID` | [Reddit Apps](https://www.reddit.com/prefs/apps) | Reddit scraping (optional) |
| `REDDIT_CLIENT_SECRET` | [Reddit Apps](https://www.reddit.com/prefs/apps) | Reddit scraping (optional) |

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

### Quick Command Reference

| Command | Description |
|---------|-------------|
| `run` | Full pipeline for individual clips (discover → classify → group → stitch) |
| `run-compilations` | Full pipeline for source compilations (download → group → stitch) |
| `reddit run` | Full Reddit pipeline (discover → TTS → compose) |
| `discover` | Find individual viral TikTok clips |
| `discover-compilations` | Find existing TikTok compilation videos |
| `reddit discover` | Scrape Reddit stories |
| `classify` | AI categorize downloaded clips |
| `group` | Group clips into compilations |
| `stitch` | Render compilations to video files |
| `reddit generate` | Generate TTS audio for Reddit posts |
| `reddit compose` | Compose Reddit narration videos |
| `review` | List compilations ready for review |
| `approve <id>` | Approve a compilation for upload |
| `reject <id>` | Reject a compilation |
| `upload <id>` | Upload compilation to YouTube |
| `status` | Show pipeline statistics |
| `reddit status` | Show Reddit pipeline statistics |
| `list-compilations` | List all compilations |
| `list-source-compilations` | List discovered source compilations |

---

### Three Pipeline Modes

The pipeline supports three modes for creating content:

| Mode | Command | Description |
|------|---------|-------------|
| **Individual Clips** | `run` | Discovers individual viral clips, classifies them, groups similar clips together |
| **Source Compilations** | `run-compilations` | Finds existing TikTok compilations and stitches them into mega-compilations |
| **Reddit Stories** | `reddit run` | Scrapes Reddit stories, generates TTS narration, composes videos with synced captions |

---

### Individual Clips Pipeline (Standard)

Discovers individual viral TikTok clips, downloads them, classifies by category, groups similar clips, and stitches into compilations.

```bash
# Full pipeline: discover -> download -> classify -> group -> stitch
python cli.py run --discover-limit 50 --compilations 2

# With specific hashtag
python cli.py run -d 50 -c 2 --hashtag fails
```

**Individual stages:**

```bash
python cli.py discover --limit 50       # Find individual viral clips
python cli.py classify                   # AI categorizes each clip
python cli.py group --compilations 3     # Group clips into compilations
python cli.py stitch                     # Render final videos
```

---

### Source Compilations Pipeline (Mega-Compilations)

Discovers existing TikTok compilations (videos that are already compilations), downloads them, and stitches multiple compilations together into longer mega-compilations.

```bash
# Full pipeline: download sources -> group -> stitch
python cli.py run-compilations --compilations 2

# With more sources per mega-compilation
python cli.py run-compilations -c 2 --sources-per 4

# Only use fails-type compilations
python cli.py run-compilations -c 2 --type fails
```

**Individual stages:**

```bash
# Discover existing compilations on TikTok
python cli.py discover-compilations --limit 30

# Discover with LLM verification and download
python cli.py discover-compilations -l 30 --classify --download

# Discover only fails compilations
python cli.py discover-compilations -l 20 --type fails

# List all discovered source compilations
python cli.py list-source-compilations
```

---

### Reddit Story Narration Pipeline

Scrapes text stories from Reddit, converts to speech with Edge TTS, and composes videos with synchronized captions over background gameplay footage.

**Prerequisites:**
1. Create a Reddit app at https://www.reddit.com/prefs/apps (select "script" type)
2. Set `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET` in `.env`
3. Add background MP4 videos to `config/backgrounds/`

```bash
# Full pipeline: discover -> TTS -> compose
python cli.py reddit run --videos 3

# From specific subreddit
python cli.py reddit run -s tifu --videos 2

# With custom discovery limit
python cli.py reddit run -d 20 -v 5
```

**Individual stages:**

```bash
# Discover stories from configured subreddits
python cli.py reddit discover --limit 10

# Discover from specific subreddit
python cli.py reddit discover -s AmItheAsshole -l 5

# Generate TTS audio
python cli.py reddit generate --limit 5

# Compose videos (requires backgrounds in config/backgrounds/)
python cli.py reddit compose --limit 3

# Check status
python cli.py reddit status

# List posts and videos
python cli.py reddit list
python cli.py reddit videos

# Approve/reject videos
python cli.py reddit approve <video_id>
python cli.py reddit reject <video_id>
```

**Configured Subreddits (in `config/reddit.yaml`):**
- r/AmItheAsshole - Moral judgment stories
- r/tifu - "Today I F***ed Up" stories
- r/relationship_advice - Relationship drama
- r/pettyrevenge - Revenge stories
- r/MaliciousCompliance - Workplace compliance stories

---

### Review & Upload Workflow

```bash
python cli.py review                    # List compilations ready for review
python cli.py approve <id>              # Approve compilation for upload
python cli.py reject <id>               # Reject compilation
python cli.py upload <id>               # Manual upload to YouTube
python cli.py upload <id> --public      # Upload as public (default: private)
```

---

### Status & Monitoring

```bash
python cli.py status                    # Show pipeline statistics
python cli.py list-compilations         # List all compilations with status
python cli.py list-source-compilations  # List discovered source compilations
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
│   ├── reddit.yaml           # Reddit subreddit configuration
│   ├── music/                # Background music (.mp3)
│   └── backgrounds/          # Background videos for Reddit narration (.mp4)
├── core/
│   ├── models.py             # Data models (Video, Compilation, RedditPost, etc.)
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
│   ├── tiktok_uploader.py    # TikTok upload via cookies
│   ├── reddit_scraper.py     # PRAW Reddit scraper
│   ├── reddit_tts.py         # Edge TTS audio generation
│   └── reddit_composer.py    # Reddit video composition
├── scheduler/
│   └── jobs.py               # APScheduler job definitions
├── data/
│   ├── downloads/            # Downloaded TikTok videos
│   ├── reddit_audio/         # Generated TTS audio files
│   └── pipeline.db           # SQLite database
├── output/
│   ├── review/               # Rendered TikTok compilations
│   └── reddit/               # Rendered Reddit narration videos
├── daemon.py                 # Background scheduler runner
├── pipeline.py               # TikTok pipeline orchestrator
├── reddit_pipeline.py        # Reddit pipeline orchestrator
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

# =============================================================================
# REDDIT STORY NARRATION
# =============================================================================
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_TTS_VOICE=en-US-ChristopherNeural  # Edge TTS voice
REDDIT_MIN_WORDS=150                       # Minimum story length
REDDIT_MAX_WORDS=500                       # Maximum story length
REDDIT_MIN_UPVOTES=1000                    # Minimum upvotes to consider
```

---

## Cost Estimates

**TikTok Compilation Pipeline:**

| Component | Cost per Run (50 videos, 2 compilations) |
|-----------|------------------------------------------|
| Apify (TikTok scraping) | ~$0.50-1.00 |
| OpenAI (classification) | ~$0.05 |
| YouTube API | Free (quota limits apply) |
| TikTok API | Free (uses cookies) |
| **Total per run** | **~$0.55-1.05** |

**Reddit Story Pipeline:**

| Component | Cost |
|-----------|------|
| Reddit API (PRAW) | Free |
| Edge TTS | Free |
| FFmpeg | Free |
| **Total** | **$0.00** |

**Monthly estimates (4 runs/day):**
- TikTok pipeline: ~$70-130/month
- Reddit pipeline: $0/month

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

### "Reddit API credentials not configured"
```bash
# 1. Create app at https://www.reddit.com/prefs/apps
# 2. Select "script" as app type
# 3. Add to .env:
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
```

### "No background videos found"
```bash
# Add MP4 files to config/backgrounds/
# Good options: Minecraft parkour, Subway Surfers, GTA driving
ls config/backgrounds/
# Should show: gameplay1.mp4, gameplay2.mp4, etc.
```

### "TTS generation failed"
- Check internet connection (Edge TTS requires online access)
- Verify post text isn't empty
- Check logs for specific error

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

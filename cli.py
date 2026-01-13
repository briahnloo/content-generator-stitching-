#!/usr/bin/env python3
"""
CLI interface for viral-clips-pipeline.
"""

import logging
import sys
from pathlib import Path

import click
from tqdm import tqdm

from config.settings import settings
from core.database import Database
from core.models import CompilationStatus, VideoStatus, Platform, ContentStrategy
from pipeline import Pipeline
from services.account_manager import AccountManager
from services.upload_router import UploadRouter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_pipeline() -> Pipeline:
    """Get pipeline instance."""
    settings.ensure_directories()
    db = Database(settings.DATABASE_PATH)
    return Pipeline(db)


def progress_bar(total: int, desc: str):
    """Create a tqdm progress bar."""
    return tqdm(total=total, desc=desc, unit="item")


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose: bool):
    """Viral Clips Pipeline - Automated TikTok to YouTube compilation generator."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


# =============================================================================
# Discovery Commands
# =============================================================================


@cli.command()
@click.option("--limit", "-l", default=50, help="Number of videos to discover")
@click.option("--hashtag", "-t", default=None, help="Specific hashtag to search")
@click.option("--no-download", is_flag=True, help="Skip downloading videos")
def discover(limit: int, hashtag: str, no_download: bool):
    """Discover and download trending TikTok videos."""
    pipeline = get_pipeline()

    click.echo(f"Discovering videos (limit={limit}, hashtag={hashtag or 'default'})...")

    with tqdm(total=limit, desc="Downloading", unit="video") as pbar:
        def progress(current, total, video):
            pbar.update(1)

        stats = pipeline.discover(
            limit=limit,
            hashtag=hashtag,
            download=not no_download,
            progress_callback=progress if not no_download else None,
        )

    click.echo("\nDiscovery Results:")
    click.echo(f"  Discovered: {stats['discovered']}")
    click.echo(f"  Skipped (duplicates): {stats['skipped_duplicates']}")
    if not no_download:
        click.echo(f"  Downloaded: {stats['downloaded']}")
        click.echo(f"  Download failed: {stats['download_failed']}")


@cli.command()
@click.option("--limit", "-l", default=30, help="Max compilations to discover")
@click.option("--type", "-t", "comp_type", default=None,
              type=click.Choice(["fails", "comedy", "satisfying"]),
              help="Specific compilation type")
@click.option("--classify", "-c", is_flag=True, help="Run LLM classification on discovered compilations")
@click.option("--download", "-d", is_flag=True, help="Download discovered compilations")
def discover_compilations(limit: int, comp_type: str, classify: bool, download: bool):
    """Discover existing TikTok compilations to stitch together.

    Finds videos that are already compilations (multiple clips edited together)
    based on metadata patterns like 'top 10', 'compilation', countdown numbers, etc.
    """
    pipeline = get_pipeline()

    click.echo("Discovering existing TikTok compilations...")
    click.echo(f"  Limit: {limit}")
    if comp_type:
        click.echo(f"  Type: {comp_type}")

    # Discover compilations
    from services.discovery import DiscoveryService
    discovery = DiscoveryService(pipeline.db)

    if comp_type:
        compilations, skipped = discovery.discover_compilations_by_type(comp_type, limit)
    else:
        compilations, skipped = discovery.discover_compilations(limit)

    click.echo(f"\nDiscovery Results:")
    click.echo(f"  Found: {len(compilations)} compilations")
    click.echo(f"  Skipped: {skipped} (duplicates/non-compilations)")

    if not compilations:
        click.echo("\nNo compilations found.")
        return

    # Show found compilations
    click.echo(f"\nDiscovered Compilations:")
    for comp in compilations:
        click.echo(f"  - {comp.id[:8]}: {comp.compilation_type or 'mixed'} "
                   f"(~{comp.source_clip_count} clips, {comp.duration:.0f}s)")
        if comp.description:
            desc_preview = comp.description[:60] + "..." if len(comp.description) > 60 else comp.description
            click.echo(f"    {desc_preview}")

    # Optional: LLM classification for quality verification
    if classify:
        click.echo(f"\nClassifying compilations with LLM...")
        from services.classifier import ClassifierService
        classifier = ClassifierService(pipeline.db)

        verified = 0
        rejected = 0

        with tqdm(total=len(compilations), desc="Verifying", unit="video") as pbar:
            for comp in compilations:
                result = classifier.classify_compilation_and_update(comp)
                if result:
                    verified += 1
                else:
                    rejected += 1
                pbar.update(1)

        click.echo(f"\nClassification Results:")
        click.echo(f"  Verified: {verified}")
        click.echo(f"  Rejected: {rejected}")

    # Optional: Download
    if download:
        click.echo(f"\nDownloading compilations...")
        from services.downloader import DownloaderService
        downloader = DownloaderService(pipeline.db)

        downloaded = 0
        failed = 0

        with tqdm(total=len(compilations), desc="Downloading", unit="video") as pbar:
            for comp in compilations:
                success = downloader.download(comp)
                if success:
                    downloaded += 1
                else:
                    failed += 1
                pbar.update(1)

        click.echo(f"\nDownload Results:")
        click.echo(f"  Downloaded: {downloaded}")
        click.echo(f"  Failed: {failed}")


@cli.command()
@click.option("--compilations", "-c", default=2, help="Number of mega-compilations to create")
@click.option("--sources-per", "-s", default=None, type=int, help="Source compilations per mega-compilation")
@click.option("--type", "-t", "comp_type", default=None,
              type=click.Choice(["fails", "comedy", "satisfying", "mixed"]),
              help="Only use sources of this type")
def run_compilations(compilations: int, sources_per: int, comp_type: str):
    """Run full pipeline for source compilations: download -> group -> stitch.

    This takes discovered source compilations (existing TikTok compilations),
    downloads them, groups them into mega-compilations, and stitches them together.

    Example:
        python cli.py run-compilations --compilations 2 --sources-per 4
    """
    pipeline = get_pipeline()

    click.echo("Starting source compilation pipeline...")
    click.echo(f"  Max mega-compilations: {compilations}")
    if sources_per:
        click.echo(f"  Sources per compilation: {sources_per}")
    if comp_type:
        click.echo(f"  Type filter: {comp_type}")
    click.echo()

    # Step 1: Download discovered source compilations that haven't been downloaded
    from core.models import VideoStatus
    from services.downloader import DownloaderService

    discovered = pipeline.db.get_source_compilations(status=VideoStatus.DISCOVERED)
    if comp_type:
        discovered = [v for v in discovered if (v.compilation_type or "mixed") == comp_type]

    if discovered:
        click.echo(f"Downloading {len(discovered)} source compilations...")
        downloader = DownloaderService(pipeline.db)

        downloaded = 0
        failed = 0

        with tqdm(total=len(discovered), desc="Downloading", unit="video") as pbar:
            for video in discovered:
                if downloader.download(video):
                    downloaded += 1
                else:
                    failed += 1
                pbar.update(1)

        click.echo(f"  Downloaded: {downloaded}")
        click.echo(f"  Failed: {failed}")
    else:
        click.echo("No new source compilations to download.")

    # Step 2: Show available sources
    from services.grouper import GrouperService
    grouper = GrouperService(pipeline.db)

    available = grouper.get_groupable_source_compilations()
    if not available:
        click.echo("\nNo downloaded source compilations available for grouping.")
        click.echo("Run 'discover-compilations -d' to discover and download more.")
        return

    click.echo(f"\nAvailable source compilations:")
    for type_name, count in sorted(available.items()):
        click.echo(f"  {type_name}: {count}")

    # Step 3: Create mega-compilations
    click.echo(f"\nCreating up to {compilations} mega-compilations...")

    if comp_type:
        # Create for specific type
        created = []
        for _ in range(compilations):
            comp = grouper.create_mega_compilation(comp_type, sources_per)
            if comp:
                created.append(comp)
            else:
                break
    else:
        created = grouper.create_mega_compilations(compilations, sources_per)

    if not created:
        click.echo("No mega-compilations created (need at least 2 source compilations).")
        return

    click.echo(f"\nCreated {len(created)} mega-compilations:")
    for comp in created:
        click.echo(f"  {comp.id}: {comp.title} ({len(comp.video_ids)} sources)")

    # Step 4: Stitch
    click.echo(f"\nStitching mega-compilations...")
    success, fail = pipeline.stitch()

    click.echo(f"\nStitching Results:")
    click.echo(f"  Success: {success}")
    click.echo(f"  Failed: {fail}")

    # Show review folder
    review_comps = pipeline.db.get_compilations_by_status(CompilationStatus.REVIEW)
    if review_comps:
        click.echo(f"\nReady for review in: {settings.REVIEW_DIR}")
        for comp in review_comps:
            click.echo(f"  {comp.id}: {comp.title}")


@cli.command()
def list_source_compilations():
    """List all discovered source compilations."""
    pipeline = get_pipeline()

    compilations = pipeline.db.get_source_compilations()

    if not compilations:
        click.echo("No source compilations found.")
        click.echo("Use 'discover-compilations' to find existing TikTok compilations.")
        return

    click.echo(f"Source Compilations ({len(compilations)}):\n")

    # Group by type
    by_type = {}
    for comp in compilations:
        comp_type = comp.compilation_type or "mixed"
        if comp_type not in by_type:
            by_type[comp_type] = []
        by_type[comp_type].append(comp)

    for comp_type, type_comps in sorted(by_type.items()):
        click.echo(f"{comp_type.upper()} ({len(type_comps)}):")
        for comp in type_comps:
            status_icon = {
                "discovered": "🔍",
                "downloaded": "⬇️",
                "classified": "✅",
                "rejected": "❌",
            }.get(comp.status.value, "❓")

            click.echo(f"  {status_icon} {comp.id[:8]}: ~{comp.source_clip_count} clips, {comp.duration:.0f}s")
            if comp.description:
                desc_preview = comp.description[:50] + "..." if len(comp.description) > 50 else comp.description
                click.echo(f"      {desc_preview}")
        click.echo()


# =============================================================================
# Classification Commands
# =============================================================================


@cli.command()
@click.option("--limit", "-l", default=None, type=int, help="Max videos to classify")
def classify(limit: int):
    """Classify downloaded videos into categories."""
    pipeline = get_pipeline()

    downloaded = pipeline.db.get_videos_by_status(VideoStatus.DOWNLOADED, limit)
    if not downloaded:
        click.echo("No downloaded videos to classify.")
        return

    click.echo(f"Classifying {len(downloaded)} videos...")

    with tqdm(total=len(downloaded), desc="Classifying", unit="video") as pbar:
        def progress(current, total, video):
            pbar.update(1)

        stats = pipeline.classify(limit=limit, progress_callback=progress)

    click.echo("\nClassification Results:")
    click.echo(f"  Classified: {stats['classified']}")
    click.echo(f"  Skipped (low confidence): {stats['skipped']}")
    click.echo(f"  Failed: {stats['failed']}")

    # Show category breakdown
    category_counts = pipeline.db.count_videos_by_category(VideoStatus.CLASSIFIED)
    if category_counts:
        click.echo("\nVideos by Category:")
        for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
            click.echo(f"  {cat}: {count}")


# =============================================================================
# Grouping Commands
# =============================================================================


@cli.command()
@click.option("--compilations", "-c", default=3, help="Max compilations to create")
@click.option("--clips", "-n", default=None, type=int, help="Clips per compilation")
def group(compilations: int, clips: int):
    """Group classified videos into compilations."""
    pipeline = get_pipeline()

    # Show available categories
    groupable = pipeline.grouper.get_groupable_categories()
    if not groupable:
        click.echo("No categories have enough videos for compilations.")
        click.echo(f"Minimum clips required: {settings.MIN_CLIPS_PER_COMPILATION}")
        return

    click.echo("Categories with enough videos:")
    for cat, count in sorted(groupable.items(), key=lambda x: -x[1]):
        click.echo(f"  {cat}: {count} available")

    click.echo(f"\nCreating up to {compilations} compilations...")

    created = pipeline.group(compilations, clips)

    if created:
        click.echo(f"\nCreated {len(created)} compilations:")
        for comp in created:
            click.echo(f"  {comp.id}: {comp.title} ({len(comp.video_ids)} clips)")
    else:
        click.echo("No compilations created.")


# =============================================================================
# Stitching Commands
# =============================================================================


@cli.command()
@click.option("--id", "compilation_id", default=None, help="Specific compilation ID")
def stitch(compilation_id: str):
    """Render pending compilations into videos."""
    pipeline = get_pipeline()

    if compilation_id:
        comp = pipeline.db.get_compilation(compilation_id)
        if not comp:
            click.echo(f"Compilation {compilation_id} not found.")
            return

        click.echo(f"Rendering compilation: {comp.title}")

        def progress(step, total, desc):
            click.echo(f"  [{step}/{total}] {desc}")

        success, fail = pipeline.stitch(compilation_id, progress)
    else:
        pending = pipeline.db.get_compilations_by_status(CompilationStatus.PENDING)
        if not pending:
            click.echo("No pending compilations to render.")
            return

        click.echo(f"Rendering {len(pending)} pending compilations...")
        success, fail = pipeline.stitch()

    click.echo(f"\nStitching complete: {success} succeeded, {fail} failed")

    # Show review folder
    review_comps = pipeline.db.get_compilations_by_status(CompilationStatus.REVIEW)
    if review_comps:
        click.echo(f"\nReady for review in: {settings.REVIEW_DIR}")
        for comp in review_comps:
            click.echo(f"  {comp.id}: {comp.title}")


# =============================================================================
# Review Commands
# =============================================================================


@cli.command()
def review():
    """List compilations ready for review."""
    pipeline = get_pipeline()

    review_comps = pipeline.db.get_compilations_by_status(CompilationStatus.REVIEW)

    if not review_comps:
        click.echo("No compilations ready for review.")
        return

    click.echo(f"Compilations ready for review ({len(review_comps)}):\n")

    for comp in review_comps:
        click.echo(f"ID: {comp.id}")
        click.echo(f"  Title: {comp.title}")
        click.echo(f"  Category: {comp.category}")
        click.echo(f"  Duration: {comp.duration:.1f}s")
        click.echo(f"  Clips: {len(comp.video_ids)}")
        click.echo(f"  File: {comp.output_path}")
        click.echo(f"  Credits: {comp.credits_text}")
        click.echo()

    click.echo(f"Review folder: {settings.REVIEW_DIR}")
    click.echo("\nUse 'approve <id>' or 'reject <id>' to process.")


@cli.command()
@click.argument("compilation_id")
def approve(compilation_id: str):
    """Approve a compilation for upload."""
    pipeline = get_pipeline()

    if pipeline.approve(compilation_id):
        click.echo(f"Compilation {compilation_id} approved for upload.")
    else:
        click.echo(f"Failed to approve compilation {compilation_id}.")


@cli.command()
@click.argument("compilation_id")
@click.option("--keep-file", is_flag=True, help="Keep the rendered video file")
def reject(compilation_id: str, keep_file: bool):
    """Reject a compilation."""
    pipeline = get_pipeline()

    if pipeline.reject(compilation_id, delete_file=not keep_file):
        click.echo(f"Compilation {compilation_id} rejected.")
    else:
        click.echo(f"Failed to reject compilation {compilation_id}.")


# =============================================================================
# Upload Commands
# =============================================================================


@cli.command()
@click.argument("compilation_id")
@click.option("--public", is_flag=True, help="Upload as public (default: private)")
def upload(compilation_id: str, public: bool):
    """Upload an approved compilation to YouTube."""
    pipeline = get_pipeline()

    comp = pipeline.db.get_compilation(compilation_id)
    if not comp:
        click.echo(f"Compilation {compilation_id} not found.")
        return

    if comp.status != CompilationStatus.APPROVED:
        click.echo(f"Compilation must be APPROVED first (current: {comp.status.value}).")
        click.echo(f"Use: python cli.py approve {compilation_id}")
        return

    privacy = "public" if public else "private"
    click.echo(f"Uploading compilation: {comp.title} (privacy: {privacy})")

    def progress(percent):
        click.echo(f"  Upload progress: {percent}%")

    video_id = pipeline.upload(compilation_id, privacy, progress)

    if video_id:
        click.echo(f"\nUpload complete!")
        click.echo(f"Video ID: {video_id}")
        click.echo(f"URL: https://youtube.com/watch?v={video_id}")
    else:
        click.echo("Upload failed.")


@cli.command()
def auth():
    """Run YouTube OAuth authentication flow."""
    pipeline = get_pipeline()

    click.echo("Starting YouTube OAuth flow...")
    click.echo("A browser window will open for authentication.")

    if pipeline.uploader.authenticate():
        click.echo("Authentication successful!")
    else:
        click.echo("Authentication failed.")


# =============================================================================
# Full Pipeline Commands
# =============================================================================


@cli.command()
@click.option("--discover-limit", "-d", default=50, help="Videos to discover")
@click.option("--compilations", "-c", default=3, help="Compilations to create")
@click.option("--hashtag", "-t", default=None, help="Specific hashtag")
def run(discover_limit: int, compilations: int, hashtag: str):
    """Run full pipeline: discover -> classify -> group -> stitch."""
    pipeline = get_pipeline()

    click.echo("Starting full pipeline run...")
    click.echo(f"  Discover limit: {discover_limit}")
    click.echo(f"  Max compilations: {compilations}")
    click.echo(f"  Hashtag: {hashtag or 'default'}")
    click.echo()

    stats = pipeline.run(
        discover_limit=discover_limit,
        max_compilations=compilations,
        hashtag=hashtag,
    )

    click.echo("\n" + "=" * 50)
    click.echo("Pipeline Run Complete")
    click.echo("=" * 50)

    click.echo("\nDiscovery:")
    click.echo(f"  Discovered: {stats['discovery'].get('discovered', 0)}")
    click.echo(f"  Downloaded: {stats['discovery'].get('downloaded', 0)}")

    click.echo("\nClassification:")
    click.echo(f"  Classified: {stats['classification'].get('classified', 0)}")
    click.echo(f"  Skipped: {stats['classification'].get('skipped', 0)}")

    click.echo("\nGrouping:")
    click.echo(f"  Compilations created: {stats['grouping']}")

    click.echo("\nStitching:")
    click.echo(f"  Rendered: {stats['stitching'].get('success', 0)}")
    click.echo(f"  Failed: {stats['stitching'].get('failed', 0)}")

    # Show what's ready for review
    review_comps = pipeline.db.get_compilations_by_status(CompilationStatus.REVIEW)
    if review_comps:
        click.echo(f"\nReady for review ({len(review_comps)}):")
        for comp in review_comps:
            click.echo(f"  {comp.id}: {comp.title}")
        click.echo(f"\nReview folder: {settings.REVIEW_DIR}")


# =============================================================================
# Utility Commands
# =============================================================================


@cli.command()
def status():
    """Show pipeline status and statistics."""
    pipeline = get_pipeline()
    stats = pipeline.get_status()

    click.echo("Pipeline Status")
    click.echo("=" * 50)

    click.echo(f"\nTotal videos: {stats['total_videos']}")
    click.echo(f"Total compilations: {stats['total_compilations']}")

    if stats['videos_by_status']:
        click.echo("\nVideos by Status:")
        for status, count in sorted(stats['videos_by_status'].items()):
            click.echo(f"  {status}: {count}")

    if stats['videos_by_category']:
        click.echo("\nClassified Videos by Category:")
        for cat, count in sorted(stats['videos_by_category'].items(), key=lambda x: -x[1]):
            click.echo(f"  {cat}: {count}")

    if stats['compilations_by_status']:
        click.echo("\nCompilations by Status:")
        for status, count in sorted(stats['compilations_by_status'].items()):
            click.echo(f"  {status}: {count}")


@cli.command()
@click.option("--confirm", is_flag=True, help="Confirm database reset")
def reset(confirm: bool):
    """Reset the database (delete all data)."""
    if not confirm:
        click.echo("This will delete all data from the database.")
        click.echo("Run with --confirm to proceed.")
        return

    pipeline = get_pipeline()
    pipeline.reset()
    click.echo("Database reset complete.")


@cli.command()
def list_compilations():
    """List all compilations."""
    pipeline = get_pipeline()
    compilations = pipeline.db.get_all_compilations()

    if not compilations:
        click.echo("No compilations found.")
        return

    click.echo(f"All Compilations ({len(compilations)}):\n")

    for comp in compilations:
        status_emoji = {
            "pending": "⏳",
            "rendering": "🔄",
            "review": "👀",
            "approved": "✅",
            "uploaded": "📤",
            "rejected": "❌",
        }.get(comp.status.value, "❓")

        click.echo(f"{status_emoji} {comp.id}: {comp.title}")
        click.echo(f"   Status: {comp.status.value} | Category: {comp.category}")
        click.echo(f"   Clips: {len(comp.video_ids)} | Duration: {comp.duration:.1f}s")
        if comp.youtube_id:
            click.echo(f"   YouTube: https://youtube.com/watch?v={comp.youtube_id}")
        click.echo()


# =============================================================================
# Account Management Commands
# =============================================================================


@cli.group()
def account():
    """Manage platform accounts."""
    pass


@account.command("add")
@click.option("--platform", "-p", required=True, type=click.Choice(["youtube", "tiktok"]))
@click.option("--name", "-n", required=True, help="Account name")
@click.option("--strategy", "-s", default="mixed", type=click.Choice(["fails", "comedy", "mixed"]))
@click.option("--handle", "-h", default="", help="@username handle")
@click.option("--daily-limit", "-l", default=6, help="Daily upload limit")
def account_add(platform: str, name: str, strategy: str, handle: str, daily_limit: int):
    """Add a new platform account."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    platform_enum = Platform.YOUTUBE if platform == "youtube" else Platform.TIKTOK
    strategy_enum = ContentStrategy(strategy)

    account = manager.create_account(
        platform=platform_enum,
        name=name,
        strategy=strategy_enum,
        handle=handle,
        daily_limit=daily_limit,
    )

    click.echo(f"Created {platform} account: {account.name}")
    click.echo(f"  ID: {account.id}")
    click.echo(f"  Strategy: {strategy}")
    click.echo(f"  Daily limit: {daily_limit}")
    click.echo(f"\nNext: Set credentials with 'account auth {account.id}'")


@account.command("list")
@click.option("--platform", "-p", type=click.Choice(["youtube", "tiktok"]), default=None)
@click.option("--all", "show_all", is_flag=True, help="Include inactive accounts")
def account_list(platform: str, show_all: bool):
    """List all accounts."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    platform_enum = None
    if platform:
        platform_enum = Platform.YOUTUBE if platform == "youtube" else Platform.TIKTOK

    accounts = manager.list_accounts(platform=platform_enum, active_only=not show_all)

    if not accounts:
        click.echo("No accounts found.")
        return

    click.echo(f"Accounts ({len(accounts)}):\n")

    for acc in accounts:
        status_icon = "✓" if acc.is_active else "✗"
        creds_icon = "🔑" if acc.credentials_encrypted else "⚠️"

        click.echo(f"{status_icon} {acc.id}: {acc.name} ({acc.platform.value})")
        click.echo(f"   Strategy: {acc.content_strategy.value}")
        click.echo(f"   Handle: {acc.handle or 'not set'}")
        click.echo(f"   Uploads today: {acc.uploads_today}/{acc.daily_upload_limit}")
        click.echo(f"   Credentials: {creds_icon} {'set' if acc.credentials_encrypted else 'NOT SET'}")
        if acc.error:
            click.echo(f"   Error: {acc.error}")
        click.echo()


@account.command("auth")
@click.argument("account_id")
@click.option("--client-id", prompt=True, help="OAuth Client ID")
@click.option("--client-secret", prompt=True, hide_input=True, help="OAuth Client Secret")
def account_auth(account_id: str, client_id: str, client_secret: str):
    """Set up YouTube OAuth credentials for an account."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    acc = manager.get_account(account_id)
    if not acc:
        click.echo(f"Account {account_id} not found.")
        return

    if acc.platform != Platform.YOUTUBE:
        click.echo("OAuth flow is only for YouTube accounts.")
        click.echo("For TikTok, use 'account set-cookies' command.")
        return

    click.echo("Starting YouTube OAuth flow...")
    click.echo("A browser window will open for authentication.")

    from services.youtube_uploader import YouTubeUploader
    refresh_token = YouTubeUploader.run_oauth_flow(client_id, client_secret)

    if refresh_token:
        credentials = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }

        if manager.set_credentials(account_id, credentials):
            click.echo(f"Credentials stored for {acc.name}")
        else:
            click.echo("Failed to store credentials.")
    else:
        click.echo("Authentication failed.")


@account.command("set-cookies")
@click.argument("account_id")
@click.option("--browser", "-b", default="chrome", type=click.Choice(["chrome", "firefox", "edge"]))
def account_set_cookies(account_id: str, browser: str):
    """Extract and store TikTok cookies from browser."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    acc = manager.get_account(account_id)
    if not acc:
        click.echo(f"Account {account_id} not found.")
        return

    if acc.platform != Platform.TIKTOK:
        click.echo("Cookie extraction is only for TikTok accounts.")
        return

    click.echo(f"Extracting TikTok cookies from {browser}...")
    click.echo("Make sure you're logged into TikTok in that browser.")

    from services.tiktok_uploader import TikTokUploader
    cookies = TikTokUploader.extract_cookies_from_browser(browser)

    if cookies and TikTokUploader.validate_cookies(cookies):
        credentials = {"cookies": cookies}
        if manager.set_credentials(account_id, credentials):
            click.echo(f"Cookies stored for {acc.name}")
            click.echo(f"Found {len(cookies)} cookies.")
        else:
            click.echo("Failed to store cookies.")
    else:
        click.echo("Failed to extract valid cookies.")
        click.echo("Make sure you're logged into TikTok.")


@account.command("deactivate")
@click.argument("account_id")
def account_deactivate(account_id: str):
    """Deactivate an account."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    if manager.deactivate_account(account_id):
        click.echo(f"Account {account_id} deactivated.")
    else:
        click.echo(f"Failed to deactivate account {account_id}.")


@account.command("activate")
@click.argument("account_id")
def account_activate(account_id: str):
    """Activate a deactivated account."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    if manager.activate_account(account_id):
        click.echo(f"Account {account_id} activated.")
    else:
        click.echo(f"Failed to activate account {account_id}.")


@account.command("delete")
@click.argument("account_id")
@click.option("--confirm", is_flag=True, help="Confirm deletion")
def account_delete(account_id: str, confirm: bool):
    """Delete an account."""
    if not confirm:
        click.echo("Run with --confirm to delete the account.")
        return

    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    if manager.delete_account(account_id):
        click.echo(f"Account {account_id} deleted.")
    else:
        click.echo(f"Failed to delete account {account_id}.")


# =============================================================================
# Routing Rules Commands
# =============================================================================


@cli.group()
def route():
    """Manage content routing rules."""
    pass


@route.command("add")
@click.argument("account_id")
@click.option("--category", "-c", required=True, type=click.Choice(["fails", "comedy"]))
@click.option("--min-confidence", "-m", default=0.7, help="Minimum confidence threshold")
@click.option("--priority", "-p", default=1, help="Priority (higher = preferred)")
def route_add(account_id: str, category: str, min_confidence: float, priority: int):
    """Add a routing rule for an account."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    rule = manager.add_routing_rule(
        account_id=account_id,
        category=category,
        min_confidence=min_confidence,
        priority=priority,
    )

    if rule:
        click.echo(f"Added routing rule: {category} -> account {account_id}")
        click.echo(f"  Min confidence: {min_confidence}")
        click.echo(f"  Priority: {priority}")
    else:
        click.echo("Failed to add routing rule. Check account ID.")


@route.command("list")
def route_list():
    """List all routing rules."""
    db = Database(settings.DATABASE_PATH)
    rules = db.get_all_routing_rules()

    if not rules:
        click.echo("No routing rules found.")
        return

    click.echo(f"Routing Rules ({len(rules)}):\n")

    # Group by account
    by_account = {}
    for rule in rules:
        if rule.account_id not in by_account:
            by_account[rule.account_id] = []
        by_account[rule.account_id].append(rule)

    for account_id, account_rules in by_account.items():
        acc = db.get_account(account_id)
        name = acc.name if acc else account_id

        click.echo(f"{name}:")
        for rule in account_rules:
            click.echo(f"  - {rule.category}: confidence >= {rule.min_confidence}, priority {rule.priority}")
        click.echo()


@route.command("delete")
@click.argument("rule_id")
def route_delete(rule_id: str):
    """Delete a routing rule."""
    db = Database(settings.DATABASE_PATH)
    manager = AccountManager(db)

    if manager.delete_routing_rule(rule_id):
        click.echo(f"Routing rule {rule_id} deleted.")
    else:
        click.echo(f"Failed to delete routing rule {rule_id}.")


# =============================================================================
# Upload Queue Commands
# =============================================================================


@cli.group()
def queue():
    """Manage upload queue."""
    pass


@queue.command("list")
@click.option("--platform", "-p", type=click.Choice(["youtube", "tiktok"]), default=None)
def queue_list(platform: str):
    """List pending uploads."""
    db = Database(settings.DATABASE_PATH)
    router = UploadRouter(db)

    platform_enum = None
    if platform:
        platform_enum = Platform.YOUTUBE if platform == "youtube" else Platform.TIKTOK

    uploads = router.get_pending_uploads(platform=platform_enum)

    if not uploads:
        click.echo("No pending uploads.")
        return

    click.echo(f"Pending Uploads ({len(uploads)}):\n")

    for upload in uploads:
        acc = db.get_account(upload.account_id)
        comp = db.get_compilation(upload.compilation_id)

        click.echo(f"{upload.id}:")
        click.echo(f"  Compilation: {comp.title if comp else upload.compilation_id}")
        click.echo(f"  Account: {acc.name if acc else upload.account_id} ({upload.platform.value})")
        click.echo(f"  Privacy: {upload.privacy}")
        click.echo(f"  Status: {upload.status.value}")
        if upload.error:
            click.echo(f"  Error: {upload.error}")
        click.echo()


@queue.command("stats")
def queue_stats():
    """Show upload queue statistics."""
    db = Database(settings.DATABASE_PATH)
    router = UploadRouter(db)

    stats = router.get_upload_stats()

    click.echo("Upload Queue Statistics:")
    click.echo(f"  Pending:   {stats['pending']}")
    click.echo(f"  Uploading: {stats['uploading']}")
    click.echo(f"  Success:   {stats['success']}")
    click.echo(f"  Failed:    {stats['failed']}")
    click.echo(f"  Total:     {stats['total']}")


@queue.command("retry")
def queue_retry():
    """Retry failed uploads."""
    db = Database(settings.DATABASE_PATH)
    router = UploadRouter(db)

    count = router.retry_failed_uploads()
    click.echo(f"Re-queued {count} failed uploads.")


# =============================================================================
# Daemon Commands
# =============================================================================


@cli.group()
def daemon():
    """Control the background daemon."""
    pass


@daemon.command("start")
@click.option("--aggressive", is_flag=True, help="Use aggressive schedule")
def daemon_start(aggressive: bool):
    """Start the daemon (runs in foreground)."""
    import subprocess
    import os

    script = Path(__file__).parent / "daemon.py"
    args = ["python", str(script)]
    if aggressive:
        args.append("--aggressive")

    click.echo("Starting daemon...")
    click.echo("Press Ctrl+C to stop.")

    os.execvp("python", args)


@daemon.command("run-now")
def daemon_run_now():
    """Run the full pipeline once."""
    from scheduler.jobs import PipelineScheduler

    scheduler = PipelineScheduler()
    click.echo("Running full pipeline...")
    scheduler.job_full_pipeline()
    click.echo("Pipeline complete.")


@daemon.command("status")
def daemon_status():
    """Show daemon and pipeline status."""
    import subprocess

    script = Path(__file__).parent / "daemon.py"
    subprocess.run(["python", str(script), "--status"])


# =============================================================================
# Reddit Story Narration Commands
# =============================================================================


def get_reddit_pipeline():
    """Get Reddit pipeline instance."""
    from reddit_pipeline import RedditPipeline
    settings.ensure_directories()
    return RedditPipeline()


@cli.group()
def reddit():
    """Reddit story narration pipeline commands."""
    pass


@reddit.command("discover")
@click.option("--subreddit", "-s", default=None, help="Specific subreddit to scrape")
@click.option("--limit", "-l", default=10, help="Max posts per subreddit")
def reddit_discover(subreddit: str, limit: int):
    """Discover Reddit stories for narration."""
    pipeline = get_reddit_pipeline()

    click.echo("Discovering Reddit stories...")
    if subreddit:
        click.echo(f"  Subreddit: r/{subreddit}")
    else:
        click.echo("  Using configured subreddits")
    click.echo(f"  Limit: {limit}")

    try:
        posts, skipped = pipeline.discover(subreddit, limit)
        click.echo(f"\nDiscovery Results:")
        click.echo(f"  Discovered: {len(posts)}")
        click.echo(f"  Skipped: {skipped}")

        if posts:
            click.echo(f"\nDiscovered Posts:")
            for post in posts[:5]:
                click.echo(f"  - r/{post.subreddit}: {post.title[:50]}...")
                click.echo(f"    {post.word_count} words, ~{post.estimated_duration:.0f}s")
            if len(posts) > 5:
                click.echo(f"  ... and {len(posts) - 5} more")
    except Exception as e:
        click.echo(f"Error: {e}")
        raise click.Abort()


@reddit.command("generate")
@click.option("--limit", "-l", default=None, type=int, help="Max posts to process")
def reddit_generate(limit: int):
    """Generate TTS audio for discovered posts."""
    pipeline = get_reddit_pipeline()

    pending = pipeline.get_pending_posts()
    if limit:
        pending = pending[:limit]

    if not pending:
        click.echo("No posts pending TTS generation.")
        return

    click.echo(f"Generating TTS audio for {len(pending)} posts...")

    with tqdm(total=len(pending), desc="Generating", unit="post") as pbar:
        success_count = 0
        fail_count = 0

        for post in pending:
            if pipeline.tts.generate_and_update(post):
                success_count += 1
            else:
                fail_count += 1
            pbar.update(1)

    click.echo(f"\nTTS Generation Results:")
    click.echo(f"  Success: {success_count}")
    click.echo(f"  Failed: {fail_count}")


@reddit.command("compose")
@click.option("--limit", "-l", default=None, type=int, help="Max videos to compose")
def reddit_compose(limit: int):
    """Compose videos from posts with audio."""
    pipeline = get_reddit_pipeline()

    # Check for backgrounds
    from config.settings import settings
    backgrounds = list(settings.BACKGROUNDS_DIR.glob("*.mp4"))
    if not backgrounds:
        click.echo(f"No background videos found in {settings.BACKGROUNDS_DIR}")
        click.echo("Add MP4 files to use as backgrounds.")
        return

    click.echo(f"Found {len(backgrounds)} background videos.")

    audio_ready = pipeline.get_audio_ready_posts()
    if limit:
        audio_ready = audio_ready[:limit]

    if not audio_ready:
        click.echo("No posts with audio ready for composition.")
        return

    click.echo(f"Composing {len(audio_ready)} videos...")

    with tqdm(total=len(audio_ready), desc="Composing", unit="video") as pbar:
        success_count = 0
        fail_count = 0

        for post in audio_ready:
            video = pipeline.composer.compose_and_update(post)
            if video:
                success_count += 1
            else:
                fail_count += 1
            pbar.update(1)

    click.echo(f"\nComposition Results:")
    click.echo(f"  Success: {success_count}")
    click.echo(f"  Failed: {fail_count}")

    if success_count > 0:
        click.echo(f"\nVideos ready for review in: {settings.REDDIT_OUTPUT_DIR}")


@reddit.command("run")
@click.option("--subreddit", "-s", default=None, help="Specific subreddit to scrape")
@click.option("--discover-limit", "-d", default=10, help="Max posts to discover per subreddit")
@click.option("--videos", "-v", default=3, help="Max videos to create")
def reddit_run(subreddit: str, discover_limit: int, videos: int):
    """Run full Reddit pipeline: discover -> TTS -> compose."""
    pipeline = get_reddit_pipeline()

    # Check for backgrounds
    from config.settings import settings
    backgrounds = list(settings.BACKGROUNDS_DIR.glob("*.mp4"))
    if not backgrounds:
        click.echo(f"No background videos found in {settings.BACKGROUNDS_DIR}")
        click.echo("Add MP4 files to use as backgrounds before running the pipeline.")
        return

    click.echo("Starting Reddit Story Narration Pipeline...")
    click.echo(f"  Subreddit: {subreddit or 'all configured'}")
    click.echo(f"  Discover limit: {discover_limit}")
    click.echo(f"  Videos to create: {videos}")
    click.echo(f"  Backgrounds available: {len(backgrounds)}")
    click.echo()

    results = pipeline.run_full_pipeline(
        subreddit=subreddit,
        discover_limit=discover_limit,
        videos_to_create=videos,
    )

    click.echo("\n" + "=" * 50)
    click.echo("Pipeline Complete")
    click.echo("=" * 50)
    click.echo(f"Posts discovered: {results['discovered']}")
    click.echo(f"Posts skipped: {results['skipped']}")
    click.echo(f"Audio generated: {results['audio_success']}")
    click.echo(f"Videos created: {results['videos_success']}")

    if results['videos_success'] > 0:
        click.echo(f"\nVideos ready in: {settings.REDDIT_OUTPUT_DIR}")


@reddit.command("status")
def reddit_status():
    """Show Reddit pipeline status and statistics."""
    pipeline = get_reddit_pipeline()
    stats = pipeline.get_status()

    click.echo("Reddit Pipeline Status")
    click.echo("=" * 50)

    click.echo(f"\nTotal posts: {stats['total_posts']}")
    click.echo(f"Total videos: {stats['total_videos']}")

    if stats['posts_by_status']:
        click.echo("\nPosts by Status:")
        for status, count in sorted(stats['posts_by_status'].items()):
            click.echo(f"  {status}: {count}")

    if stats['videos_by_status']:
        click.echo("\nVideos by Status:")
        for status, count in sorted(stats['videos_by_status'].items()):
            click.echo(f"  {status}: {count}")


@reddit.command("list")
@click.option("--status", "-s", default=None,
              type=click.Choice(["discovered", "audio_ready", "composed", "uploaded", "failed"]),
              help="Filter by status")
@click.option("--limit", "-l", default=20, help="Max posts to show")
def reddit_list(status: str, limit: int):
    """List Reddit posts."""
    pipeline = get_reddit_pipeline()
    posts = pipeline.list_posts(status, limit)

    if not posts:
        click.echo("No Reddit posts found.")
        return

    click.echo(f"Reddit Posts ({len(posts)}):\n")

    for post in posts:
        status_icon = {
            "discovered": "🔍",
            "audio_ready": "🔊",
            "composed": "🎬",
            "uploaded": "📤",
            "failed": "❌",
        }.get(post.status.value, "❓")

        click.echo(f"{status_icon} {post.id}: r/{post.subreddit}")
        click.echo(f"   {post.title[:60]}...")
        click.echo(f"   {post.word_count} words, ~{post.estimated_duration:.0f}s, {post.upvotes} upvotes")
        click.echo()


@reddit.command("videos")
@click.option("--status", "-s", default=None,
              type=click.Choice(["pending", "review", "approved", "uploaded", "rejected"]),
              help="Filter by status")
@click.option("--limit", "-l", default=20, help="Max videos to show")
def reddit_videos(status: str, limit: int):
    """List Reddit videos."""
    pipeline = get_reddit_pipeline()
    videos = pipeline.list_videos(status, limit)

    if not videos:
        click.echo("No Reddit videos found.")
        return

    click.echo(f"Reddit Videos ({len(videos)}):\n")

    for video in videos:
        status_icon = {
            "pending": "⏳",
            "review": "👀",
            "approved": "✅",
            "uploaded": "📤",
            "rejected": "❌",
        }.get(video.status.value, "❓")

        click.echo(f"{status_icon} {video.id}: {video.title[:50]}...")
        click.echo(f"   Duration: {video.duration:.0f}s")
        click.echo(f"   Output: {video.output_path}")
        click.echo()


@reddit.command("approve")
@click.argument("video_id")
def reddit_approve(video_id: str):
    """Approve a Reddit video for upload."""
    pipeline = get_reddit_pipeline()

    if pipeline.approve_video(video_id):
        click.echo(f"Video {video_id} approved for upload.")
    else:
        click.echo(f"Failed to approve video {video_id}.")


@reddit.command("reject")
@click.argument("video_id")
def reddit_reject(video_id: str):
    """Reject a Reddit video."""
    pipeline = get_reddit_pipeline()

    if pipeline.reject_video(video_id):
        click.echo(f"Video {video_id} rejected.")
    else:
        click.echo(f"Failed to reject video {video_id}.")


if __name__ == "__main__":
    cli()

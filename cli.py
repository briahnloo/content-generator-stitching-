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
from core.models import CompilationStatus, VideoStatus
from pipeline import Pipeline

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


if __name__ == "__main__":
    cli()

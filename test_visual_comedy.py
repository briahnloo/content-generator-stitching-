#!/usr/bin/env python3
"""
Test script to discover and process visual comedy content.
Focuses on content that works without audio (physical comedy, reactions, pranks).
"""

import logging
import sys
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from core.database import Database
from core.models import VideoStatus, CompilationStatus
from config.settings import settings
from services.discovery import DiscoveryService
from services.downloader import DownloaderService
from services.classifier import ClassifierService
from services.grouper import GrouperService
from services.stitcher import StitcherService


# Hashtags focused on visual comedy (no audio needed)
VISUAL_COMEDY_HASHTAGS = [
    "fails",           # Physical fails - universally funny
    "instantregret",   # Immediate visual payoff
    "wcgw",            # What could go wrong - visual mishaps
    "faceplant",       # Physical comedy
    "epicfail",        # Classic fail content
    "funnyvideos",     # General funny
    "trynottolaugh",   # Compilation-ready content
    "unexpected",      # Surprise moments
    "pranks",          # Visual pranks
    "gotcha",          # Prank reactions
]


def discover_visual_comedy(db: Database, limit_per_hashtag: int = 10) -> int:
    """Discover videos focused on visual comedy."""
    discovery = DiscoveryService(db)
    total_discovered = 0

    for hashtag in VISUAL_COMEDY_HASHTAGS:
        logger.info(f"Discovering #{hashtag}...")
        try:
            videos, skipped = discovery.discover_by_hashtag(
                hashtag=hashtag,
                limit=limit_per_hashtag
            )
            total_discovered += len(videos)
            logger.info(f"  Found {len(videos)} new, {skipped} skipped")
        except Exception as e:
            logger.error(f"  Error discovering #{hashtag}: {e}")

    return total_discovered


def download_discovered(db: Database, limit: int = 50) -> tuple:
    """Download discovered videos."""
    downloader = DownloaderService(db)
    success, failed = downloader.download_discovered(limit=limit)
    logger.info(f"Downloaded {success} videos, {failed} failed")
    return success, failed


def classify_for_visual_comedy(db: Database, limit: int = 50) -> tuple:
    """
    Classify videos with emphasis on visual comedy.
    Uses higher thresholds for compilation_score and visual_independence.
    Only accepts content that works without audio/context.
    """
    classifier = ClassifierService(db)

    # Classify with strict thresholds for visual-only content
    # - min_compilation_score=0.6: must work as standalone clip
    # - min_visual_independence=0.6: must be funny on mute
    classified, skipped, failed = classifier.classify_downloaded(
        limit=limit,
        min_confidence=0.5,
        min_compilation_score=0.6,
        min_visual_independence=0.6  # NEW: Must pass the "mute test"
    )

    logger.info(f"Classified {classified}, skipped {skipped}, failed {failed}")
    return classified, skipped, failed


def create_visual_comedy_compilation(db: Database) -> str:
    """Create a compilation from visual comedy content."""
    grouper = GrouperService(db, min_compilation_score=0.5, min_visual_independence=0.5)

    # Check what we have
    stats = grouper.get_compilation_stats()
    logger.info(f"Available videos by category: {stats['by_category']}")

    if stats.get('by_subcategory'):
        logger.info(f"By subcategory: {stats['by_subcategory']}")

    # Prefer fails (more visual) or comedy
    # Try to create from physical subcategory first
    compilation = None

    # Try comedy/physical or comedy/reaction (visual subcategories)
    for category in ['comedy', 'fails']:
        subcats = db.get_available_subcategories(category, min_videos=5)
        if subcats:
            # Prefer physical or reaction subcategories (most visual)
            for preferred in ['physical', 'reaction', 'skill', 'prank']:
                if preferred in subcats:
                    compilation = grouper.create_compilation_by_subcategory(
                        category, preferred, num_clips=5
                    )
                    if compilation:
                        break

        if compilation:
            break

    # Fallback to any category with enough videos
    if not compilation:
        for category in ['fails', 'comedy']:
            videos = db.get_videos_by_category(
                category,
                status=VideoStatus.CLASSIFIED,
                unassigned_only=True
            )
            if len(videos) >= 5:
                compilation = grouper.create_compilation(category, num_clips=5)
                if compilation:
                    break

    if compilation:
        logger.info(f"Created compilation: {compilation.title} ({compilation.id})")
        return compilation.id
    else:
        logger.warning("Could not create compilation - not enough videos")
        return None


def render_compilation(db: Database, compilation_id: str) -> bool:
    """Render a compilation to the review folder."""
    stitcher = StitcherService(db)

    compilation = db.get_compilation(compilation_id)
    if not compilation:
        logger.error(f"Compilation {compilation_id} not found")
        return False

    logger.info(f"Rendering: {compilation.title}")

    def progress(step, total, msg):
        logger.info(f"  [{step}/{total}] {msg}")

    success = stitcher.render(compilation, progress_callback=progress)

    if success:
        # Reload to get output path
        compilation = db.get_compilation(compilation_id)
        logger.info(f"Rendered to: {compilation.output_path}")
        return True
    else:
        logger.error("Render failed")
        return False


def main():
    """Run the visual comedy test pipeline."""
    print("=" * 60)
    print("Visual Comedy Test Pipeline")
    print("=" * 60)

    # Initialize database
    db = Database(settings.DATABASE_PATH)

    # Step 1: Discover visual comedy content
    print("\n[1/4] Discovering visual comedy content...")
    discovered = discover_visual_comedy(db, limit_per_hashtag=5)
    print(f"      Discovered {discovered} new videos")

    # Step 2: Download
    print("\n[2/4] Downloading videos...")
    downloaded, failed = download_discovered(db, limit=30)
    print(f"      Downloaded {downloaded}, failed {failed}")

    # Step 3: Classify
    print("\n[3/4] Classifying for visual comedy...")
    classified, skipped, failed = classify_for_visual_comedy(db, limit=30)
    print(f"      Classified {classified}, skipped {skipped}, failed {failed}")

    # Step 4: Create and render compilation
    print("\n[4/4] Creating and rendering compilation...")
    compilation_id = create_visual_comedy_compilation(db)

    if compilation_id:
        success = render_compilation(db, compilation_id)
        if success:
            compilation = db.get_compilation(compilation_id)
            print(f"\n" + "=" * 60)
            print("SUCCESS!")
            print(f"Output: {compilation.output_path}")
            print(f"Title: {compilation.title}")
            print(f"Duration: {compilation.duration:.1f}s")
            print("=" * 60)
            return 0

    print("\nNo compilation created - may need more qualifying videos")

    # Show what we have
    print("\nCurrent classified videos by category:")
    for cat in ['fails', 'comedy']:
        videos = db.get_videos_by_category(cat, status=VideoStatus.CLASSIFIED, unassigned_only=True)
        if videos:
            print(f"  {cat}: {len(videos)} videos")
            for v in videos[:3]:
                print(f"    - {v.subcategory or 'no-subcat'}: {v.description[:50]}...")

    return 1


if __name__ == "__main__":
    sys.exit(main())

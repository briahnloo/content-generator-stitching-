"""
Configuration management for viral-clips-pipeline.
Loads environment variables and provides centralized settings access.
"""

import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv
import yaml

# Base paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"

# Load environment variables - explicitly from project root
env_path = BASE_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    # Fallback to default behavior (current directory)
    load_dotenv()


def _get_env(key: str, default: str = None, required: bool = False) -> str:
    """Get environment variable with optional default and required check."""
    value = os.getenv(key, default)
    if required and not value:
        raise ValueError(f"Required environment variable {key} is not set")
    return value


def _get_env_int(key: str, default: int) -> int:
    """Get integer environment variable."""
    return int(os.getenv(key, str(default)))


def _get_env_float(key: str, default: float) -> float:
    """Get float environment variable."""
    return float(os.getenv(key, str(default)))


def _get_env_list(key: str, default: str) -> List[str]:
    """Get comma-separated list from environment variable."""
    value = os.getenv(key, default)
    return [item.strip() for item in value.split(",") if item.strip()]


class Settings:
    """Application settings loaded from environment variables."""

    # API Keys
    APIFY_API_TOKEN: str = _get_env("APIFY_API_TOKEN", required=False)
    OPENAI_API_KEY: str = _get_env("OPENAI_API_KEY", required=False)

    # YouTube OAuth (optional)
    YOUTUBE_CLIENT_ID: str = _get_env("YOUTUBE_CLIENT_ID", "")
    YOUTUBE_CLIENT_SECRET: str = _get_env("YOUTUBE_CLIENT_SECRET", "")
    YOUTUBE_REFRESH_TOKEN: str = _get_env("YOUTUBE_REFRESH_TOKEN", "")

    # Paths
    DATABASE_PATH: Path = Path(_get_env("DATABASE_PATH", str(BASE_DIR / "data" / "pipeline.db")))
    DOWNLOAD_DIR: Path = Path(_get_env("DOWNLOAD_DIR", str(BASE_DIR / "data" / "downloads")))
    OUTPUT_DIR: Path = Path(_get_env("OUTPUT_DIR", str(BASE_DIR / "output")))
    REVIEW_DIR: Path = Path(_get_env("REVIEW_DIR", str(BASE_DIR / "output" / "review")))
    MUSIC_DIR: Path = Path(_get_env("MUSIC_DIR", str(CONFIG_DIR / "music")))

    # Video Settings
    VIDEO_WIDTH: int = _get_env_int("VIDEO_WIDTH", 1080)
    VIDEO_HEIGHT: int = _get_env_int("VIDEO_HEIGHT", 1920)
    FPS: int = _get_env_int("FPS", 30)
    MAX_CLIP_DURATION: float = _get_env_float("MAX_CLIP_DURATION", 15.0)

    # Compilation Settings (5 clips @ ~15s each = ~75s total, within 60-90s target)
    MIN_CLIPS_PER_COMPILATION: int = _get_env_int("MIN_CLIPS_PER_COMPILATION", 5)
    MAX_CLIPS_PER_COMPILATION: int = _get_env_int("MAX_CLIPS_PER_COMPILATION", 5)
    MIN_CLASSIFICATION_CONFIDENCE: float = _get_env_float("MIN_CLASSIFICATION_CONFIDENCE", 0.3)

    # Discovery
    DISCOVERY_HASHTAGS: List[str] = _get_env_list(
        "DISCOVERY_HASHTAGS",
        "viral,fyp,trending,fails,satisfying"
    )

    # Download retry settings
    MAX_DOWNLOAD_RETRIES: int = _get_env_int("MAX_DOWNLOAD_RETRIES", 3)

    # OpenAI settings
    OPENAI_MODEL: str = _get_env("OPENAI_MODEL", "gpt-4o-mini")

    @classmethod
    def ensure_directories(cls) -> None:
        """Create all required directories if they don't exist."""
        cls.DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        cls.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        cls.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cls.REVIEW_DIR.mkdir(parents=True, exist_ok=True)
        cls.MUSIC_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate_api_keys(cls, require_apify: bool = False, require_openai: bool = False) -> None:
        """Validate that required API keys are set."""
        if require_apify and not cls.APIFY_API_TOKEN:
            raise ValueError("APIFY_API_TOKEN is required but not set")
        if require_openai and not cls.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY is required but not set")


class CategoriesConfig:
    """Category configuration loaded from YAML."""

    _config: dict = None

    @classmethod
    def _load(cls) -> dict:
        """Load categories configuration from YAML file."""
        if cls._config is None:
            config_path = CONFIG_DIR / "categories.yaml"
            if config_path.exists():
                with open(config_path, "r") as f:
                    cls._config = yaml.safe_load(f)
            else:
                cls._config = cls._default_config()
        return cls._config

    @classmethod
    def _default_config(cls) -> dict:
        """Return default configuration if YAML doesn't exist."""
        return {
            "categories": {
                "fails": {
                    "name": "Epic Fails",
                    "keywords": ["fail", "fails", "accident", "gone wrong", "oops", "instant regret"],
                    "hashtags": ["#fail", "#epicfail", "#fails", "#instantregret"],
                    "compilation_titles": [
                        "Try Not To Laugh: {count} Epic Fails",
                        "Fails of the Day #{part}",
                        "When Things Go Wrong #{part}",
                        "Instant Regret Compilation #{part}"
                    ],
                    "mood": "upbeat"
                },
                "comedy": {
                    "name": "Comedy Gold",
                    "keywords": ["funny", "comedy", "hilarious", "lol", "humor", "joke", "meme"],
                    "hashtags": ["#funny", "#comedy", "#humor", "#memes"],
                    "compilation_titles": [
                        "Try Not To Laugh #{part}",
                        "{count} Hilarious Moments",
                        "Comedy Gold Compilation #{part}",
                        "Funny Moments #{part}"
                    ],
                    "mood": "upbeat"
                }
            },
            "music_moods": {
                "upbeat": ["energetic.mp3", "fun.mp3", "happy.mp3", "comedic.mp3"]
            }
        }

    @classmethod
    def get_categories(cls) -> dict:
        """Get all category configurations."""
        return cls._load().get("categories", {})

    @classmethod
    def get_category(cls, name: str) -> dict:
        """Get a specific category configuration."""
        return cls.get_categories().get(name, {})

    @classmethod
    def get_category_names(cls) -> List[str]:
        """Get list of all category names."""
        return list(cls.get_categories().keys())

    @classmethod
    def get_music_for_mood(cls, mood: str) -> List[str]:
        """Get music files for a specific mood."""
        moods = cls._load().get("music_moods", {})
        return moods.get(mood, [])

    @classmethod
    def get_compilation_title(cls, category: str, count: int, part: int = 1) -> str:
        """Generate a compilation title from templates."""
        import random
        config = cls.get_category(category)
        templates = config.get("compilation_titles", ["{category} Compilation #{part}"])
        template = random.choice(templates)
        return template.format(
            count=count,
            part=part,
            category=config.get("name", category.title())
        )


# Singleton instance
settings = Settings()
categories_config = CategoriesConfig()

"""
Configuration management for viral-clips-pipeline.
Loads environment variables and provides centralized settings access.
"""

import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv
import yaml

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"


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

    # Compilation Settings
    MIN_CLIPS_PER_COMPILATION: int = _get_env_int("MIN_CLIPS_PER_COMPILATION", 5)
    MAX_CLIPS_PER_COMPILATION: int = _get_env_int("MAX_CLIPS_PER_COMPILATION", 8)
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

    # Encryption
    CREDENTIALS_ENCRYPTION_KEY: str = _get_env("CREDENTIALS_ENCRYPTION_KEY", "")

    # Auto-approval settings
    AUTO_APPROVE_THRESHOLD: float = _get_env_float("AUTO_APPROVE_THRESHOLD", 0.75)
    MIN_CONFIDENCE_FOR_UPLOAD: float = _get_env_float("MIN_CONFIDENCE_FOR_UPLOAD", 0.6)

    # Rate limits
    YOUTUBE_DAILY_LIMIT_PER_ACCOUNT: int = _get_env_int("YOUTUBE_DAILY_LIMIT_PER_ACCOUNT", 3)
    TIKTOK_DAILY_LIMIT_PER_ACCOUNT: int = _get_env_int("TIKTOK_DAILY_LIMIT_PER_ACCOUNT", 5)

    # Scheduler settings
    SCHEDULER_ENABLED: bool = _get_env("SCHEDULER_ENABLED", "true").lower() == "true"
    DISCOVERY_INTERVAL_HOURS: int = _get_env_int("DISCOVERY_INTERVAL_HOURS", 4)
    UPLOAD_INTERVAL_MINUTES: int = _get_env_int("UPLOAD_INTERVAL_MINUTES", 15)

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
                    "keywords": ["fail", "accident", "gone wrong", "oops"],
                    "hashtags": ["#fail", "#epicfail", "#fails"],
                    "compilation_titles": [
                        "Try Not To Laugh: {count} Epic Fails",
                        "Fails of the Day #{part}",
                        "When Things Go Wrong #{part}"
                    ],
                    "mood": "upbeat"
                },
                "satisfying": {
                    "name": "Oddly Satisfying",
                    "keywords": ["satisfying", "asmr", "relaxing", "smooth"],
                    "hashtags": ["#satisfying", "#oddlysatisfying", "#asmr"],
                    "compilation_titles": [
                        "Oddly Satisfying Videos #{part}",
                        "{count} Satisfying Moments to Relax",
                        "Most Satisfying Compilation #{part}"
                    ],
                    "mood": "chill"
                },
                "wholesome": {
                    "name": "Wholesome Moments",
                    "keywords": ["wholesome", "heartwarming", "cute", "sweet", "faith in humanity"],
                    "hashtags": ["#wholesome", "#heartwarming", "#feelgood"],
                    "compilation_titles": [
                        "Wholesome Moments That Made My Day #{part}",
                        "{count} Heartwarming Clips",
                        "Faith in Humanity Restored #{part}"
                    ],
                    "mood": "emotional"
                },
                "comedy": {
                    "name": "Comedy Gold",
                    "keywords": ["funny", "comedy", "hilarious", "lol", "humor", "joke"],
                    "hashtags": ["#funny", "#comedy", "#humor"],
                    "compilation_titles": [
                        "Try Not To Laugh #{part}",
                        "{count} Hilarious Moments",
                        "Comedy Gold Compilation #{part}"
                    ],
                    "mood": "upbeat"
                },
                "skills": {
                    "name": "Amazing Skills",
                    "keywords": ["skill", "talent", "amazing", "impressive", "pro", "expert"],
                    "hashtags": ["#skills", "#talent", "#amazing"],
                    "compilation_titles": [
                        "People Are Awesome #{part}",
                        "{count} Incredible Skills",
                        "Next Level Talent #{part}"
                    ],
                    "mood": "hype"
                },
                "animals": {
                    "name": "Animal Moments",
                    "keywords": ["animal", "pet", "dog", "cat", "wildlife", "cute animal"],
                    "hashtags": ["#animals", "#pets", "#cute"],
                    "compilation_titles": [
                        "Cutest Animal Moments #{part}",
                        "{count} Adorable Pet Clips",
                        "Animals Being Derps #{part}"
                    ],
                    "mood": "chill"
                },
                "food": {
                    "name": "Food Content",
                    "keywords": ["food", "cooking", "recipe", "eating", "mukbang", "foodie"],
                    "hashtags": ["#food", "#cooking", "#foodie"],
                    "compilation_titles": [
                        "Food Videos That Hit Different #{part}",
                        "{count} Delicious Moments",
                        "Cooking Compilation #{part}"
                    ],
                    "mood": "chill"
                },
                "drama": {
                    "name": "Drama & Confrontations",
                    "keywords": ["drama", "confrontation", "argument", "fight", "exposed", "caught"],
                    "hashtags": ["#drama", "#exposed", "#caught"],
                    "compilation_titles": [
                        "Drama Compilation #{part}",
                        "{count} Intense Moments",
                        "Caught on Camera #{part}"
                    ],
                    "mood": "dramatic"
                }
            },
            "music_moods": {
                "upbeat": ["energetic.mp3", "fun.mp3", "happy.mp3"],
                "chill": ["lofi.mp3", "ambient.mp3", "calm.mp3"],
                "emotional": ["piano.mp3", "inspirational.mp3"],
                "hype": ["epic.mp3", "intense.mp3"],
                "dramatic": ["suspense.mp3", "tension.mp3"]
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

    @classmethod
    def get_subcategories(cls, category: str) -> dict:
        """Get subcategories for a category."""
        config = cls.get_category(category)
        return config.get("subcategories", {})

    @classmethod
    def get_subcategory_names(cls, category: str) -> List[str]:
        """Get list of subcategory names for a category."""
        return list(cls.get_subcategories(category).keys())

    @classmethod
    def get_subcategory(cls, category: str, subcategory: str) -> dict:
        """Get a specific subcategory configuration."""
        return cls.get_subcategories(category).get(subcategory, {})

    @classmethod
    def get_hard_reject_keywords(cls) -> List[str]:
        """Get keywords that should always be rejected."""
        rejection = cls._load().get("rejection_criteria", {})
        return rejection.get("hard_reject", [])

    @classmethod
    def get_soft_reject_keywords(cls) -> List[str]:
        """Get keywords that should be rejected unless strong signal."""
        rejection = cls._load().get("rejection_criteria", {})
        return rejection.get("soft_reject", [])

    @classmethod
    def get_positive_signals(cls, category: str) -> List[str]:
        """Get positive signals for a category."""
        signals = cls._load().get("positive_signals", {})
        return signals.get(category, [])

    @classmethod
    def get_compilation_quality_signals(cls) -> dict:
        """Get quality signals for compilation suitability."""
        return cls._load().get("compilation_quality", {
            "high_quality": [],
            "low_quality": []
        })

    @classmethod
    def get_narrative_rejection(cls) -> dict:
        """Get narrative/trend rejection patterns."""
        return cls._load().get("narrative_rejection", {
            "trend_patterns": [],
            "narrative_keywords": [],
            "narrative_hashtags": []
        })

    @classmethod
    def get_trend_patterns(cls) -> List[str]:
        """Get regex patterns for trend content detection."""
        rejection = cls.get_narrative_rejection()
        return rejection.get("trend_patterns", [])

    @classmethod
    def get_narrative_keywords(cls) -> List[str]:
        """Get keywords indicating narrative content."""
        rejection = cls.get_narrative_rejection()
        return rejection.get("narrative_keywords", [])

    @classmethod
    def get_narrative_hashtags(cls) -> List[str]:
        """Get hashtags indicating narrative/trend content."""
        rejection = cls.get_narrative_rejection()
        return rejection.get("narrative_hashtags", [])

    @classmethod
    def get_visual_signals(cls) -> dict:
        """Get visual independence signals."""
        return cls._load().get("visual_signals", {
            "high_visual_independence": [],
            "low_visual_independence": []
        })


# Singleton instance
settings = Settings()
categories_config = CategoriesConfig()

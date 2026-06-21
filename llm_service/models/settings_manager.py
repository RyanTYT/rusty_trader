# llm_service/models/settings_manager.py
#
# Settings are stored in /data/settings/settings.json on the named volume.
# They persist across container restarts and can be updated via the API
# without a redeploy — the backend reads fresh on each relevant call.

import os
from pathlib import Path

import aiofiles

from models.types import Settings

SETTINGS_PATH = Path(os.getenv("SETTINGS_PATH", "/data/settings")) / "settings.json"


async def load_settings() -> Settings:
    """Load settings from disk, returning defaults if file doesn't exist."""
    if not SETTINGS_PATH.exists():
        default = Settings()
        await save_settings(default)
        return default
    async with aiofiles.open(SETTINGS_PATH, "r") as f:
        raw = await f.read()
    return Settings.model_validate_json(raw)


async def save_settings(settings: Settings) -> None:
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(SETTINGS_PATH, "w") as f:
        await f.write(settings.model_dump_json(indent=2))


async def update_settings(**kwargs) -> Settings:
    """Merge kwargs into current settings and persist."""
    current = await load_settings()
    updated = current.model_copy(update=kwargs)
    await save_settings(updated)
    return updated

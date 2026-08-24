# llm_service/api/kb_browser.py
#
# REST endpoints for browsing and reading the knowledge base filesystem.
# Mounted onto the FastAPI app in main.py.
#
# GET /kb/tree          — full directory tree with file metadata
# GET /kb/file?path=... — read a single .md or .json file

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from tools.kb_manager import KB_ROOT

router = APIRouter(prefix="/kb", tags=["kb_browser"])

# ── Response models ────────────────────────────────────────────────────────────


class KBFile(BaseModel):
    name: str
    path: str  # Relative to KB_ROOT — used as the key for /kb/file
    size_bytes: int
    last_modified: str  # ISO timestamp
    is_dir: bool
    children: Optional[list["KBFile"]] = None


KBFile.model_rebuild()


class KBFileContent(BaseModel):
    path: str
    content: str
    size_bytes: int
    last_modified: str


# ── Tree endpoint ──────────────────────────────────────────────────────────────


@router.get("/tree", response_model=list[KBFile])
async def get_tree():
    """
    Return the full KB directory tree.
    Directories include their children recursively.
    Files include size and last_modified timestamp.
    """
    if not KB_ROOT.exists():
        return []
    return _build_tree(KB_ROOT, KB_ROOT)


def _build_tree(path: Path, root: Path) -> list[KBFile]:
    items = []
    try:
        # Dirs first, then files, both alphabetical
        entries = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        return []

    for entry in entries:
        rel = str(entry.relative_to(root))
        stat = entry.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()

        if entry.is_dir():
            items.append(
                KBFile(
                    name=entry.name,
                    path=rel,
                    size_bytes=0,
                    last_modified=mtime,
                    is_dir=True,
                    children=_build_tree(entry, root),
                )
            )
        else:
            items.append(
                KBFile(
                    name=entry.name,
                    path=rel,
                    size_bytes=stat.st_size,
                    last_modified=mtime,
                    is_dir=False,
                )
            )

    return items


# ── File read endpoint ─────────────────────────────────────────────────────────


@router.get("/file", response_model=KBFileContent)
async def get_file(path: str = Query(..., description="Path relative to KB root")):
    """
    Read a single file. Path is relative to KB_ROOT (e.g. 'macro/us/overview.md').
    Only .md and .json files are readable.
    Path traversal outside KB_ROOT is blocked.
    """
    try:
        full_path = (KB_ROOT / path).resolve()
        full_path.relative_to(KB_ROOT.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="Path traversal not allowed")

    if not full_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")

    if full_path.is_dir():
        raise HTTPException(status_code=400, detail="Path is a directory, not a file")

    if full_path.suffix.lower() not in {".md", ".json"}:
        raise HTTPException(
            status_code=400, detail=f"File type {full_path.suffix} not readable"
        )

    stat = full_path.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
    content = full_path.read_text(encoding="utf-8", errors="replace")

    return KBFileContent(
        path=path,
        content=content,
        size_bytes=stat.st_size,
        last_modified=mtime,
    )

from __future__ import annotations

"""
CLI: Export "Use This" playblasts from an ftrack folder subtree to disk.

Usage:
    python mroya_export_playblasts.py "<project>/<...path...>" <dest-dir>

For every AssetVersion under the given context whose status is "Use This",
pick up its .mov / .mp4 file components and .png / .exr sequence components,
then copy them into <dest-dir> with stable names:

    <dest>/<shot>_<asset>.mov                              (movies)
    <dest>/<shot>_<asset>_2.mov                            (extra movies)
    <dest>/<shot>_<asset>/<shot>_<asset>.0001.png          (sequences)

Filenames carry no version, so re-running overwrites old files when the
"Use This" pointer moves to a new version. Sequence folders are wiped before
they are repopulated, so stale frames cannot survive a version downgrade.

Components only on backup/S3 trigger a ``mroya.transfer.request`` to the
running Transfer Manager and are skipped this run. Re-run once transfers finish.
"""

import argparse
import json
import logging
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Iterable, List, Optional, Set


# --------------------------------------------------------------------------- #
# Bootstrap (mirrors run_browser.py)                                          #
# --------------------------------------------------------------------------- #

if sys.version_info >= (3, 12) and "imp" not in sys.modules:
    import types

    class _ImpStub:
        @staticmethod
        def find_module(name, path=None): return None

        @staticmethod
        def load_module(name, file=None, pathname=None, description=None):
            raise ImportError("imp.load_module is not supported in Python 3.12+")

        @staticmethod
        def new_module(name): return types.ModuleType(name)

        @staticmethod
        def get_suffixes(): return []

        @staticmethod
        def acquire_lock(): pass

        @staticmethod
        def release_lock(): pass

    sys.modules["imp"] = _ImpStub()  # type: ignore[assignment]


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(dotenv_path=str(path))
        return
    except Exception:
        pass
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip("'").strip('"')
        if k:
            os.environ.setdefault(k, v)


def _find_project_root(start: Path) -> Path:
    """Walk up from `start` to find the mroya project root.

    Project root is the directory that contains both ``ftrack_plugins`` and
    ``config``. Works whether this script lives at the repo top, inside a
    plugin, or anywhere else under the tree.
    """
    for p in [start] + list(start.parents):
        if (p / "ftrack_plugins").is_dir() and (p / "config").is_dir():
            return p
    return start


def _bootstrap(project_root: Path) -> None:
    _load_dotenv(project_root / ".env")
    _load_dotenv(project_root / "config" / ".env")
    _load_dotenv(
        project_root / "ftrack_plugins" / "multi-site-location-0.2.0" / ".env"
    )

    cfg = project_root / "config" / "mroya.json"
    if cfg.is_file():
        try:
            data = json.loads(cfg.read_text(encoding="utf-8"))
            for k, v in data.items():
                os.environ.setdefault(str(k), str(v))
        except Exception as e:
            print(f"[mroya_export] warn: failed to read {cfg}: {e}")

    plugins = project_root / "ftrack_plugins"
    if plugins.is_dir():
        os.environ.setdefault("FTRACK_CONNECT_PLUGIN_PATH", str(plugins))
        if str(plugins) not in sys.path:
            sys.path.insert(0, str(plugins))
        for sub in (
            plugins / "ftrack_inout" / "dependencies",
            plugins / "multi-site-location-0.2.0" / "dependencies",
            plugins / "mroya_transfer_manager" / "dependencies",
        ):
            if sub.is_dir() and str(sub) not in sys.path:
                sys.path.insert(0, str(sub))


# --------------------------------------------------------------------------- #
# Constants                                                                   #
# --------------------------------------------------------------------------- #

MOVIE_EXTS = {".mov", ".mp4"}
SEQUENCE_EXTS = {".png", ".exr"}

# Built-in ftrack locations that don't represent a real storage we can copy
# from (or that hold encoded review media we explicitly want to ignore).
BUILTIN_LOCATIONS = {
    "ftrack.origin",
    "ftrack.unmanaged",
    "ftrack.review",
    "ftrack.server",
    "ftrack.connect",
}

log = logging.getLogger("mroya_export")


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

_SAFE_RX = re.compile(r"[^A-Za-z0-9._-]+")


def _norm_ext(file_type: Optional[str]) -> str:
    s = (file_type or "").strip().lower()
    if s and not s.startswith("."):
        s = "." + s
    return s


def _sanitize(name: str) -> str:
    return _SAFE_RX.sub("_", (name or "").strip()) or "unknown"


def resolve_project_path(session: Any, project_path: str) -> Any:
    """Resolve a "Project/Seq/Shot/..." string to a context entity."""
    parts = [p for p in re.split(r"[\\/]", project_path) if p]
    if not parts:
        raise ValueError("Empty project path")

    proj_name = parts[0]
    proj = session.query(
        f'Project where full_name is "{proj_name}"'
    ).first() or session.query(
        f'Project where name is "{proj_name}"'
    ).first()
    if proj is None:
        raise ValueError(f"Project not found: {proj_name!r}")

    current = proj
    for seg in parts[1:]:
        child = session.query(
            f'TypedContext where parent.id is "{current["id"]}" '
            f'and name is "{seg}"'
        ).first()
        if child is None:
            raise ValueError(
                f"Context not found under {current.get('name', '?')}: {seg!r}"
            )
        current = child
    return current


def subtree_ids(session: Any, root_id: str) -> List[str]:
    """BFS down `TypedContext.parent.id` to collect every descendant + root."""
    out = [root_id]
    frontier: List[str] = [root_id]
    while frontier:
        ids_clause = ", ".join(f'"{x}"' for x in frontier)
        rows = session.query(
            f"select id from TypedContext where parent.id in ({ids_clause})"
        ).all()
        new_ids = [r["id"] for r in rows]
        if not new_ids:
            break
        out.extend(new_ids)
        frontier = new_ids
    return out


def query_use_this_versions(
    session: Any, parent_ids: Iterable[str]
) -> List[Any]:
    """Versions whose status is 'Use This' for assets under the given parents."""
    out: List[Any] = []
    parent_ids = list(parent_ids)
    CHUNK = 200
    for i in range(0, len(parent_ids), CHUNK):
        chunk = parent_ids[i:i + CHUNK]
        ids = ", ".join(f'"{p}"' for p in chunk)
        rows = session.query(
            "select id, version, asset.name, asset.parent.name, "
            "components.id, components.name, components.file_type, "
            "components.padding, "
            "components.component_locations.location.id, "
            "components.component_locations.location.name "
            "from AssetVersion "
            f'where status.name is "Use This" and asset.parent.id in ({ids})'
        ).all()
        out.extend(rows)
    return out


def classify_component(comp: Any) -> Optional[str]:
    """Return 'movie', 'sequence', or None to skip."""
    et = getattr(comp, "entity_type", None)
    ext = _norm_ext(comp.get("file_type"))
    if et == "FileComponent" and ext in MOVIE_EXTS:
        return "movie"
    if et == "SequenceComponent" and ext in SEQUENCE_EXTS:
        return "sequence"
    return None


def non_builtin_locations(comp: Any) -> List[Any]:
    out: List[Any] = []
    for cl in comp.get("component_locations") or []:
        loc = cl.get("location")
        if not loc:
            continue
        name = loc.get("name") or ""
        if name in BUILTIN_LOCATIONS:
            continue
        out.append(loc)
    return out


def get_primary_disk_location(session: Any) -> Optional[Any]:
    """Pick the Disk location with highest precedence (lowest priority).

    Inlined rather than imported to keep the CLI independent from path_resolution.
    """
    try:
        import ftrack_api  # type: ignore
    except Exception:
        return None
    try:
        locations = session.query("Location").all()
    except Exception as e:
        log.warning("query locations failed: %s", e)
        return None

    disk_locations = []
    for loc in locations:
        name = loc.get("name") or ""
        if name in BUILTIN_LOCATIONS:
            continue
        acc = getattr(loc, "accessor", None)
        if not acc:
            continue
        if hasattr(ftrack_api.accessor, "disk") and isinstance(
            acc, ftrack_api.accessor.disk.DiskAccessor
        ):
            disk_locations.append(loc)
    if not disk_locations:
        return None
    disk_locations.sort(key=lambda l: getattr(l, "priority", 999))
    return disk_locations[0]


# --------------------------------------------------------------------------- #
# Export                                                                      #
# --------------------------------------------------------------------------- #

class ExportContext:
    def __init__(self, session: Any, dest: Path, primary_loc: Any) -> None:
        self.session = session
        self.dest = dest
        self.primary = primary_loc
        self.primary_id = primary_loc["id"]
        self.primary_name = primary_loc.get("name", "?")

        self.copied_movies = 0
        self.copied_sequences = 0
        self.copied_frames = 0
        self.queued_transfers = 0
        self.skipped: List[str] = []

    def export(self, comp: Any, out_name: str, kind: str) -> None:
        try:
            availability = self.primary.get_component_availability(comp)
        except Exception as e:
            self.skipped.append(
                f"{out_name}: availability check failed ({e})"
            )
            return

        if availability < 100.0:
            source = self._find_source_with_file(comp)
            if source is None:
                self.skipped.append(
                    f"{out_name}: not fully available in any configured location"
                )
                return
            self._queue_transfer(comp, source, out_name)
            return

        if kind == "movie":
            self._copy_movie(comp, out_name)
        else:
            self._copy_sequence(comp, out_name)

    def _find_source_with_file(self, comp: Any) -> Optional[Any]:
        for loc in non_builtin_locations(comp):
            if loc["id"] == self.primary_id:
                continue
            try:
                av = loc.get_component_availability(comp)
            except Exception:
                continue
            if av and float(av) >= 100.0:
                return loc
        return None

    def _queue_transfer(
        self, comp: Any, source_loc: Any, basename: str
    ) -> None:
        try:
            from ftrack_inout.publisher.core.transfer_after_publish import (  # type: ignore
                create_transfer_job,
            )
        except Exception as e:
            self.skipped.append(
                f"{basename}: cannot import create_transfer_job ({e})"
            )
            return
        try:
            job_id = create_transfer_job(
                self.session,
                comp["id"],
                from_location_id=source_loc["id"],
                to_location_id=self.primary_id,
                component_label=comp.get("name") or basename,
                to_location_name=self.primary_name,
            )
        except Exception as e:
            self.skipped.append(f"{basename}: queue transfer failed ({e})")
            return
        if job_id:
            self.queued_transfers += 1
            log.info(
                "queued transfer %s -> %s for %s (job %s)",
                source_loc.get("name", "?"),
                self.primary_name,
                basename,
                str(job_id)[:8],
            )
        else:
            self.skipped.append(f"{basename}: transfer job not created")

    def _copy_movie(self, comp: Any, out_name: str) -> None:
        ext = _norm_ext(comp.get("file_type"))
        try:
            src = self.primary.get_filesystem_path(comp)
        except Exception as e:
            self.skipped.append(f"{out_name}: resolve path failed ({e})")
            return
        if not src or not os.path.exists(src):
            self.skipped.append(f"{out_name}: source missing ({src})")
            return
        dst = self.dest / f"{out_name}{ext}"
        try:
            self.dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        except Exception as e:
            self.skipped.append(f"{out_name}: copy failed ({e})")
            return
        self.copied_movies += 1
        log.info("copied movie -> %s", dst)

    def _copy_sequence(self, comp: Any, out_name: str) -> None:
        ext = _norm_ext(comp.get("file_type"))
        members = comp.get("members") or []
        if not members:
            try:
                self.session.populate([comp], "members")
                members = comp.get("members") or []
            except Exception as e:
                self.skipped.append(
                    f"{out_name}: cannot load sequence members ({e})"
                )
                return
        if not members:
            self.skipped.append(f"{out_name}: sequence has no members")
            return

        padding = int(comp.get("padding") or 4)
        out_dir = self.dest / out_name

        # Wipe stale frames so a downgrade to a shorter version doesn't leak
        # frames from the previous "Use This".
        if out_dir.exists():
            try:
                shutil.rmtree(out_dir)
            except Exception as e:
                self.skipped.append(
                    f"{out_name}: failed to clear stale folder ({e})"
                )
                return
        out_dir.mkdir(parents=True, exist_ok=True)

        copied = 0
        for idx, m in enumerate(members):
            try:
                src = self.primary.get_filesystem_path(m)
            except Exception as e:
                log.warning("%s: frame resolve failed (%s)", out_name, e)
                continue
            if not src or not os.path.exists(src):
                log.warning("%s: frame missing on disk (%s)", out_name, src)
                continue
            frame = _frame_number(m, src, fallback=idx + 1)
            dst = out_dir / f"{out_name}.{frame:0{padding}d}{ext}"
            try:
                shutil.copy2(src, dst)
                copied += 1
            except Exception as e:
                log.warning(
                    "%s frame %d: copy failed (%s)", out_name, frame, e
                )

        if copied:
            self.copied_sequences += 1
            self.copied_frames += copied
            log.info("copied sequence -> %s (%d frames)", out_dir, copied)
        else:
            self.skipped.append(f"{out_name}: no frames copied")


def _frame_number(member: Any, src: str, fallback: int) -> int:
    raw = member.get("name") or ""
    try:
        return int(raw)
    except (TypeError, ValueError):
        pass
    m = re.search(r"(\d+)(?=\.[^.]+$)", os.path.basename(src))
    return int(m.group(1)) if m else fallback


# --------------------------------------------------------------------------- #
# Main                                                                       #
# --------------------------------------------------------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            'Export "Use This" playblasts (.mov/.mp4) and image sequences '
            "(.png/.exr) from an ftrack folder subtree."
        )
    )
    parser.add_argument(
        "project_path",
        help='Slash-separated context path, e.g. "MyShow/SEQ_010".',
    )
    parser.add_argument(
        "dest_dir",
        help="Destination folder on disk. Created if missing.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Verbose logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="[%(levelname)s] %(message)s",
    )

    # Create destination + log file up-front so the file captures everything,
    # including session bootstrap and any failure before the first copy.
    dest = Path(args.dest_dir).resolve()
    try:
        dest.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log.error("cannot create destination %s: %s", dest, e)
        return 1
    log_path = dest / "mroya_export.log"
    file_handler = logging.FileHandler(str(log_path), mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logging.getLogger().addHandler(file_handler)
    log.info("log file: %s", log_path)

    project_root = _find_project_root(Path(__file__).resolve().parent)
    log.info("project root: %s", project_root)
    _bootstrap(project_root)

    try:
        import ftrack_api  # noqa: F401  # type: ignore
    except Exception as e:
        log.error("ftrack_api unavailable after bootstrap: %s", e)
        return 1

    try:
        from ftrack_inout.common.session_factory import (  # type: ignore
            get_shared_session,
        )
    except Exception as e:
        log.error("import ftrack_inout.common.session_factory failed: %s", e)
        return 1

    session = get_shared_session()
    if session is None:
        log.error("could not create ftrack session (check FTRACK_* env vars)")
        return 1

    primary = get_primary_disk_location(session)
    if primary is None:
        log.error(
            "no primary Disk location configured. "
            "Configure disk_locations.yaml (e.g. burlin.local)."
        )
        return 1
    log.info("primary disk location: %s", primary.get("name"))

    try:
        root = resolve_project_path(session, args.project_path)
    except ValueError as e:
        log.error("%s", e)
        return 1
    log.info("root context: %s (%s)", root.get("name"), root["id"])

    ids = subtree_ids(session, root["id"])
    log.info("descendants (incl. root): %d", len(ids))

    versions = query_use_this_versions(session, ids)
    log.info('"Use This" versions found: %d', len(versions))
    if not versions:
        log.info("nothing to do.")
        return 0

    ctx = ExportContext(session, dest, primary)

    # Pre-classify components per version, populate sequence members in bulk.
    all_sequence_comps: List[Any] = []
    plans: List[tuple] = []  # (version, basename, movies, sequences)
    for v in versions:
        asset = v.get("asset") or {}
        asset_name = asset.get("name") or "asset"
        parent = asset.get("parent") or {}
        shot_name = parent.get("name") or "shot"
        basename = f"{_sanitize(shot_name)}_{_sanitize(asset_name)}"

        movies: List[Any] = []
        sequences: List[Any] = []
        for comp in v.get("components") or []:
            kind = classify_component(comp)
            if kind is None:
                continue
            if not non_builtin_locations(comp):
                # Server-side review encodes (ftrack.review / ftrack.server)
                # would otherwise consume a _N suffix slot and never copy.
                log.debug(
                    "skip component %s (%s): only on builtin locations",
                    comp.get("id"), comp.get("name"),
                )
                continue
            if kind == "movie":
                movies.append(comp)
            else:
                sequences.append(comp)
        movies.sort(key=lambda c: c.get("name") or "")
        sequences.sort(key=lambda c: c.get("name") or "")
        all_sequence_comps.extend(sequences)
        plans.append((v, basename, movies, sequences))

    if all_sequence_comps:
        try:
            session.populate(all_sequence_comps, "members")
        except Exception as e:
            log.warning("bulk populate sequence members failed: %s", e)

    seen_basenames: Set[str] = set()
    for v, basename, movies, sequences in plans:
        if basename in seen_basenames:
            log.warning(
                "basename %r already seen this run; output will overwrite",
                basename,
            )
        seen_basenames.add(basename)

        for i, comp in enumerate(movies):
            out_name = basename if i == 0 else f"{basename}_{i + 1}"
            ctx.export(comp, out_name, "movie")
        for i, comp in enumerate(sequences):
            out_name = basename if i == 0 else f"{basename}_{i + 1}"
            ctx.export(comp, out_name, "sequence")

    log.info(
        "Copied %d movies, %d sequences (%d frames)",
        ctx.copied_movies,
        ctx.copied_sequences,
        ctx.copied_frames,
    )
    if ctx.queued_transfers:
        log.info(
            "Queued %d transfer(s); re-run after they complete.",
            ctx.queued_transfers,
        )
    if ctx.skipped:
        log.info("Skipped %d:", len(ctx.skipped))
        for s in ctx.skipped:
            log.info("  - %s", s)
    return 0


if __name__ == "__main__":
    sys.exit(main())

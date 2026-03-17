#!/usr/bin/env python3
"""
Run the Vinyl Critics vs Streams ETL pipeline.

The goal is boring reliability:
- run the existing scripts in a clear order
- stop on the first failure
- print the exact commands being executed

Optional extras:
- match_artists_offline.py can be included with --run-offline-match
- verify_manifest.py can be run with --old-manifest and --new-manifest
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, List, Tuple

PIPELINE: List[Tuple[str, str]] = [
    ("extract_pitchfork", "extract_pitchfork.py"),
    ("inspect_pitchfork", "inspect_pitchfork.py"),
    ("stage_reviews", "stage_reviews.py"),
    ("make_review_artists_bridge", "make_review_artists_bridge.py"),
    ("build_artist_universe", "build_artist_universe.py"),
    ("clean_spotify_youtube", "clean_spotify_youtube.py"),
    ("match_artists", "match_artists.py"),
    ("load_reviews_and_bridge", "load_reviews_and_bridge.py"),
    ("load_dim_artist", "load_dim_artist.py"),
    ("stage_to_sqlite", "stage_to_sqlite.py"),
    ("validate_dw", "validate_dw.py"),
]

OPTIONAL_STEPS: List[Tuple[str, str]] = [
    ("match_artists_offline", "match_artists_offline.py"),
    ("verify_manifest", "verify_manifest.py"),
]


def scripts_dir() -> Path:
    return Path(__file__).resolve().parent


def repo_root() -> Path:
    return scripts_dir().parent


def step_names(steps: List[Tuple[str, str]]) -> List[str]:
    return [name for name, _ in steps]


def filter_steps(
    steps: List[Tuple[str, str]],
    start: str | None,
    stop: str | None,
    skip: Iterable[str],
) -> List[Tuple[str, str]]:
    skip_set = set(skip)
    available = step_names(steps)

    if start and start not in available:
        raise ValueError(f"Unknown --from '{start}'. Available: {', '.join(available)}")
    if stop and stop not in available:
        raise ValueError(f"Unknown --until '{stop}'. Available: {', '.join(available)}")

    selected = steps[:]

    if start:
        i = next(i for i, (name, _) in enumerate(selected) if name == start)
        selected = selected[i:]

    if stop:
        j = next(i for i, (name, _) in enumerate(selected) if name == stop)
        selected = selected[: j + 1]

    return [(name, fn) for (name, fn) in selected if name not in skip_set]


def run_script(script_file: Path, args: List[str], dry_run: bool) -> int:
    cmd = [sys.executable, str(script_file), *args]
    print(f"$ {' '.join(cmd)}")

    if dry_run:
        return 0

    # Run from repo root so all scripts using relative paths behave the same way.
    proc = subprocess.run(cmd, cwd=str(repo_root()))
    return proc.returncode


def ensure_files_exist(files: List[Path]) -> List[Path]:
    return [p for p in files if not p.exists()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Vinyl Critics vs Streams ETL pipeline.")
    parser.add_argument("--list", action="store_true", help="List steps and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands only, do not run.")
    parser.add_argument("--from", dest="start", help="Start from this step (inclusive).")
    parser.add_argument("--until", dest="stop", help="Stop at this step (inclusive).")
    parser.add_argument("--skip", action="append", default=[], help="Skip a step name. Repeatable.")
    parser.add_argument(
        "--run-offline-match",
        action="store_true",
        help="Also run match_artists_offline.py after match_artists.py.",
    )
    parser.add_argument(
        "--old-manifest",
        type=str,
        help="Old manifest JSON path (enables verify_manifest.py).",
    )
    parser.add_argument(
        "--new-manifest",
        type=str,
        help="New manifest JSON path (enables verify_manifest.py).",
    )
    parser.add_argument(
        "passthrough",
        nargs=argparse.REMAINDER,
        help="Args after '--' are passed to every pipeline script.",
    )
    args = parser.parse_args()

    if args.list:
        print("Pipeline steps:")
        for name, fn in PIPELINE:
            print(f"  - {name:24s} ({fn})")
        print("\nOptional steps:")
        for name, fn in OPTIONAL_STEPS:
            print(f"  - {name:24s} ({fn})")
        return 0

    passthrough = list(args.passthrough)
    if passthrough and passthrough[0] == "--":
        passthrough = passthrough[1:]

    try:
        selected = filter_steps(PIPELINE, args.start, args.stop, args.skip)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    # Decide which optional steps should run.
    run_offline_match = bool(args.run_offline_match)
    run_manifest_check = bool(args.old_manifest and args.new_manifest)

    if (args.old_manifest and not args.new_manifest) or (
        args.new_manifest and not args.old_manifest
    ):
        print(
            "Error: verify_manifest.py needs both --old-manifest and --new-manifest.",
            file=sys.stderr,
        )
        return 2

    # Check that required scripts exist.
    required_paths = [scripts_dir() / fn for _, fn in selected]
    missing_required = ensure_files_exist(required_paths)
    if missing_required:
        print("Error: Missing script files in scripts/:", file=sys.stderr)
        for p in missing_required:
            print(f"  - {p.name}", file=sys.stderr)
        return 2

    # Check optional scripts only if requested.
    if run_offline_match:
        p = scripts_dir() / "match_artists_offline.py"
        if not p.exists():
            print(
                "Error: --run-offline-match was set but match_artists_offline.py is missing.",
                file=sys.stderr,
            )
            return 2

    if run_manifest_check:
        p = scripts_dir() / "verify_manifest.py"
        if not p.exists():
            print(
                "Error: manifest check requested but verify_manifest.py is missing.",
                file=sys.stderr,
            )
            return 2

    print("Vinyl Critics vs Streams — pipeline run")
    print(f"Repo root:   {repo_root()}")
    print(f"Scripts dir: {scripts_dir()}")
    print("Steps to run:")
    for name, fn in selected:
        print(f"  - {name} ({fn})")
    if run_offline_match:
        print("  - match_artists_offline (match_artists_offline.py)")
    if run_manifest_check:
        print("  - verify_manifest (verify_manifest.py)")
    if passthrough:
        print(f"Passthrough args: {' '.join(passthrough)}")
    if args.dry_run:
        print("Dry run enabled (no scripts will actually run).")

    started = time.time()
    total_steps = len(selected) + (1 if run_offline_match else 0) + (1 if run_manifest_check else 0)
    step_no = 0

    for name, fn in selected:
        step_no += 1
        step_started = time.time()
        print(f"\n=== [{step_no}/{total_steps}] {name} ===")
        code = run_script(scripts_dir() / fn, passthrough, args.dry_run)

        if code != 0:
            took = time.time() - step_started
            print(f"\nFailed at step '{name}' (exit {code}) after {took:.1f}s.", file=sys.stderr)
            return code

        took = time.time() - step_started
        print(f"Done: {name} ({took:.1f}s)")

        # Optional offline match right after the main match step.
        if name == "match_artists" and run_offline_match:
            step_no += 1
            off_started = time.time()
            print(f"\n=== [{step_no}/{total_steps}] match_artists_offline ===")
            code2 = run_script(
                scripts_dir() / "match_artists_offline.py", passthrough, args.dry_run
            )
            if code2 != 0:
                took2 = time.time() - off_started
                print(
                    f"\nFailed at step 'match_artists_offline' (exit {code2}) after {took2:.1f}s.",
                    file=sys.stderr,
                )
                return code2
            took2 = time.time() - off_started
            print(f"Done: match_artists_offline ({took2:.1f}s)")

    # Optional manifest comparison (runs at the very end).
    if run_manifest_check:
        step_no += 1
        man_started = time.time()
        print(f"\n=== [{step_no}/{total_steps}] verify_manifest ===")
        man_args = [args.old_manifest, args.new_manifest]
        code = run_script(scripts_dir() / "verify_manifest.py", man_args, args.dry_run)
        if code != 0:
            took = time.time() - man_started
            print(
                f"\nFailed at step 'verify_manifest' (exit {code}) after {took:.1f}s.",
                file=sys.stderr,
            )
            return code
        took = time.time() - man_started
        print(f"Done: verify_manifest ({took:.1f}s)")

    total = time.time() - started
    print(f"\nAll done in {total:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

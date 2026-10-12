#!/usr/bin/env python3
"""
Put the committed samples back after scripts/break_samples.py.

Copies every file under data/samples/.backup/ back over its original, checks
the restored bytes are identical to the backup, and removes the backup
directory once everything is back. If there is no backup — the drills were
applied some other way — it falls back to `git checkout -- data/samples/`,
which restores the committed versions.

Exit code 0 means every sample is pristine again; 1 means something is still
wrong and the message says what.

Usage:
    python scripts/restore_samples.py
    python scripts/restore_samples.py --check    # report only, change nothing
"""

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "data" / "samples"
BACKUP = SAMPLES / ".backup"


def digest(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def backed_up_files() -> list[Path]:
    if not BACKUP.is_dir():
        return []
    return sorted(p for p in BACKUP.rglob("*.parquet") if p.is_file())


def restore_from_backup(check_only: bool) -> int:
    problems = 0
    for backup in backed_up_files():
        original = SAMPLES / backup.relative_to(BACKUP)
        rel = original.relative_to(ROOT)
        if original.exists() and digest(original) == digest(backup):
            print(f"{rel}: already pristine")
            continue
        if check_only:
            print(f"{rel}: BROKEN (differs from backup)")
            problems += 1
            continue
        original.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup, original)
        if digest(original) == digest(backup):
            print(f"{rel}: restored")
        else:
            print(f"{rel}: restore FAILED, bytes differ from backup", file=sys.stderr)
            problems += 1

    if not check_only and problems == 0:
        shutil.rmtree(BACKUP)
        print("backup directory removed; samples are pristine")
    return 1 if problems else 0


def restore_from_git(check_only: bool) -> int:
    """No backup to restore from: lean on git, which has the committed copies."""
    rel = str(SAMPLES.relative_to(ROOT))
    status = subprocess.run(
        ["git", "status", "--porcelain", "--", rel], cwd=ROOT, capture_output=True, text=True
    )
    if status.returncode != 0:
        print("no backup under data/samples/.backup/ and git is unavailable; nothing to restore from", file=sys.stderr)
        return 1
    if not status.stdout.strip():
        print("no backup present and git reports data/samples/ unchanged; nothing to do")
        return 0
    if check_only:
        print("no backup present; git reports modified samples:\n" + status.stdout.rstrip())
        return 1
    subprocess.run(["git", "checkout", "--", rel], cwd=ROOT, check=True)
    print("no backup present; restored data/samples/ from git")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true", help="report whether the samples are pristine; change nothing")
    args = parser.parse_args()

    if backed_up_files():
        return restore_from_backup(check_only=args.check)
    return restore_from_git(check_only=args.check)


if __name__ == "__main__":
    sys.exit(main())

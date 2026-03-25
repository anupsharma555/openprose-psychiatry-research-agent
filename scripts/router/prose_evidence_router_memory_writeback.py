#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Write evidence_router comparison lessons into run_memory.json.")
    ap.add_argument("--compare-input", required=True)
    ap.add_argument("--router-shadow-input", required=True)
    ap.add_argument("--memory-path", required=True)
    args = ap.parse_args()

    compare = load_json(args.compare_input)
    router = load_json(args.router_shadow_input)
    memory = load_json(args.memory_path) if Path(args.memory_path).exists() else {}

    lessons = memory.get("evidence_router_lessons", [])
    lesson = {
        "timestamp": utc_now_iso(),
        "topic": router.get("topic"),
        "summary": compare.get("summary", {}),
        "high_confidence_router_counts": router.get("summary", {}),
        "sample_disagreements": (compare.get("disagreements") or [])[:5],
    }
    lessons.append(lesson)
    memory["evidence_router_lessons"] = lessons[-20:]

    ensure_parent_dir(args.memory_path)
    Path(args.memory_path).write_text(json.dumps(memory, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"evidence_router_lessons_count": len(memory["evidence_router_lessons"])}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

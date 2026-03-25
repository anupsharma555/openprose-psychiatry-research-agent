#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import shutil
import subprocess
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


DEFAULT_OUTBOUND_DIR = "/home/openclaw/.openclaw/workspace/outbound"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Post a prose research report to Discord via direct openclaw message send.")
    p.add_argument("--delivery-json", required=True, help="Workflow-local delivery sidecar JSON")
    p.add_argument("--outbound-dir", default=DEFAULT_OUTBOUND_DIR, help="Outbound directory approved for media posting")
    p.add_argument("--write", default="", help="Optional output JSON path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    delivery = load_json(args.delivery_json)
    if not delivery:
        raise SystemExit(f"Could not load delivery JSON: {args.delivery_json}")

    report_md = compact_ws(delivery.get("report_md"))
    digest_md = compact_ws(delivery.get("digest_md"))
    channel_id = compact_ws(delivery.get("discord_channel_id"))
    message = compact_ws(delivery.get("message"))

    if not report_md:
        raise SystemExit("Missing report_md in delivery JSON")
    if not digest_md:
        raise SystemExit("Missing digest_md in delivery JSON")
    if not channel_id:
        raise SystemExit("Missing discord_channel_id in delivery JSON")

    report_path = Path(report_md).expanduser().resolve()
    digest_path = Path(digest_md).expanduser().resolve()

    if not report_path.exists():
        raise SystemExit(f"Report markdown not found: {report_path}")
    if not digest_path.exists():
        raise SystemExit(f"Digest markdown not found: {digest_path}")

    outbound_dir = Path(args.outbound_dir).expanduser().resolve()
    outbound_dir.mkdir(parents=True, exist_ok=True)

    staged_report = outbound_dir / "prose-research-latest.md"
    shutil.copy2(report_path, staged_report)

    digest_text = digest_path.read_text(encoding="utf-8").rstrip()
    post_msg = f"{message}\n\n{digest_text}"

    cmd = [
        "/usr/bin/openclaw",
        "message",
        "send",
        "--channel", "discord",
        "--target", channel_id,
        "--message", post_msg,
        "--media", str(staged_report),
    ]
    subprocess.run(cmd, check=True)

    output = {
        "schema_version": "1.0",
        "stage": "discord_post",
        "generated_at": utc_now_iso(),
        "delivery_json": args.delivery_json,
        "channel_id": channel_id,
        "message": message,
        "report_md": str(report_path),
        "digest_md": str(digest_path),
        "staged_report_md": str(staged_report),
        "openclaw_cmd": cmd,
    }

    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

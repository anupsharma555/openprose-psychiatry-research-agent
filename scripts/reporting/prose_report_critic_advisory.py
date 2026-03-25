#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


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
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def preferred_python(default_python: str) -> str:
    venv_python = Path(__file__).resolve().parent / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return default_python


def run_cmd(cmd: list[str], dry_run: bool = False) -> None:
    print("$ " + " ".join(cmd))
    if not dry_run:
        subprocess.run(cmd, check=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run the report_critic advisory flow.")
    p.add_argument("--report-input", required=True)
    p.add_argument("--report-md", required=True)
    p.add_argument("--digest-md", required=True)
    p.add_argument("--critic-model", default="gpt-5-mini")
    p.add_argument("--critic-model-alias", default="report_critic_model")
    p.add_argument("--python-bin", default=sys.executable)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write", required=True, help="Summary JSON path")
    return p


def main() -> int:
    args = build_parser().parse_args()
    python_bin = preferred_python(args.python_bin)

    write_path = Path(args.write)
    ensure_parent_dir(str(write_path))
    stem = write_path.stem
    parent = write_path.parent

    runtime_input = parent / f"{stem}.runtime_input.json"
    critic_shadow = parent / f"{stem}.critic_shadow.json"
    promoted_input = parent / f"{stem}.promoted_input.json"

    runtime_cmd = [
        python_bin,
        "scripts/reporting/prose_report_critic_runtime_input.py",
        "--report-input", args.report_input,
        "--report-md", args.report_md,
        "--digest-md", args.digest_md,
        "--write", str(runtime_input),
    ]
    run_cmd(runtime_cmd, dry_run=args.dry_run)

    critic_cmd = [
        python_bin,
        "scripts/reporting/prose_report_critic_agent.py",
        "--runtime-input", str(runtime_input),
        "--model", args.critic_model,
        "--model-alias", args.critic_model_alias,
        "--write", str(critic_shadow),
    ]
    run_cmd(critic_cmd, dry_run=args.dry_run)

    promote_cmd = [
        python_bin,
        "scripts/reporting/prose_report_critic_promote.py",
        "--report-input", args.report_input,
        "--critic-shadow-input", str(critic_shadow),
        "--write", str(promoted_input),
    ]
    run_cmd(promote_cmd, dry_run=args.dry_run)

    summary = {
        "runtime_input": str(runtime_input),
        "critic_shadow": str(critic_shadow),
        "promoted_input": str(promoted_input),
        "promotion_count": None,
        "use_promoted_input": False,
    }

    if not args.dry_run:
        promoted = load_json(str(promoted_input))
        promotions = promoted.get("report_critic_promotions") or []
        summary["promotion_count"] = len(promotions)
        summary["use_promoted_input"] = len(promotions) > 0

    write_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

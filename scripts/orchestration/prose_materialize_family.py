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


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def ensure_parent_dir(path_str: str) -> None:
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def preferred_python(default_python: str) -> str:
    venv_python = Path(__file__).resolve().parent / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return default_python


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Materialize a specific family from planner_family_eval as a single-family override.")
    p.add_argument("--family-eval", required=True)
    p.add_argument("--family-id", required=True)
    p.add_argument("--orchestration-plan", required=True)
    p.add_argument("--artifact-tag", default="", help="Optional explicit artifact tag for generated planner_selected_* files")
    p.add_argument("--python-bin", default=sys.executable)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write", required=True)
    return p


def main() -> int:
    args = build_parser().parse_args()

    eval_payload = load_json(args.family_eval)
    if not eval_payload:
        raise SystemExit(f"Could not load family eval: {args.family_eval}")

    family_id = compact_ws(args.family_id)
    family_ids = [compact_ws(x.get("family_id")) for x in eval_payload.get("candidate_family_results", [])]
    if family_id not in family_ids:
        raise SystemExit(f"Family id not found in family eval: {family_id}")

    override = dict(eval_payload)
    override["selected_strategy"] = "single_family"
    override["selected_family_id"] = family_id
    override["rationale"] = f"Manual single-family materialization override for {family_id}"

    override_path = Path(args.write).with_suffix(".override_eval.json")
    ensure_parent_dir(str(override_path))
    override_path.write_text(json.dumps(override, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    python_bin = preferred_python(args.python_bin)

    cmd = [
        python_bin,
        "prose_hybrid_materialize.py",
        "--family-eval", str(override_path),
        "--orchestration-plan", args.orchestration_plan,
        "--write", args.write,
    ]
    artifact_tag = compact_ws(args.artifact_tag)
    if artifact_tag:
        cmd.extend(["--artifact-tag", artifact_tag])
    if args.dry_run:
        cmd.append("--dry-run")

    print("$ " + " ".join(cmd))
    if not args.dry_run:
        subprocess.run(cmd, check=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

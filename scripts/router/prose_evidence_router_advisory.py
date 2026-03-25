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


DEFAULT_ROUTER_MODEL = "gpt-5-mini"


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
    p = argparse.ArgumentParser(description="Run the evidence_router advisory pipeline and produce an advisory-promoted portfolio input.")
    p.add_argument("--controller-input", required=True)
    p.add_argument("--coverage-input", required=True)
    p.add_argument("--direct-evidence-input", required=True)
    p.add_argument("--related-evidence-input", required=True)
    p.add_argument("--portfolio-input", required=True)
    p.add_argument("--orchestration-plan", required=True)
    p.add_argument("--memory-path", default=".prose/memory/run_memory.json")
    p.add_argument("--router-model", default=DEFAULT_ROUTER_MODEL)
    p.add_argument("--router-model-alias", default="evidence_router_model")
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

    prepared_direct = parent / f"{stem}.direct_prepared.json"
    prepared_related = parent / f"{stem}.related_prepared.json"
    runtime_input = parent / f"{stem}.runtime_input.json"
    router_shadow = parent / f"{stem}.router_shadow.json"
    compare_json = parent / f"{stem}.compare.json"
    advisory_input = parent / f"{stem}.advisory_input.json"

    prepare_direct_cmd = [
        python_bin,
        "prose_evidence_prepare.py",
        "--evidence-input", args.direct_evidence_input,
        "--write", str(prepared_direct),
    ]
    run_cmd(prepare_direct_cmd, dry_run=args.dry_run)

    prepare_related_cmd = [
        python_bin,
        "prose_evidence_prepare.py",
        "--evidence-input", args.related_evidence_input,
        "--write", str(prepared_related),
    ]
    run_cmd(prepare_related_cmd, dry_run=args.dry_run)

    runtime_cmd = [
        python_bin,
        "prose_evidence_router_runtime_input.py",
        "--controller-input", args.controller_input,
        "--coverage-input", args.coverage_input,
        "--primary-evidence-input", str(prepared_direct),
        "--secondary-evidence-input", str(prepared_related),
        "--orchestration-plan", args.orchestration_plan,
        "--write", str(runtime_input),
    ]
    run_cmd(runtime_cmd, dry_run=args.dry_run)

    router_cmd = [
        python_bin,
        "prose_evidence_router_agent.py",
        "--runtime-input", str(runtime_input),
        "--model", args.router_model,
        "--model-alias", args.router_model_alias,
        "--write", str(router_shadow),
    ]
    run_cmd(router_cmd, dry_run=args.dry_run)

    compare_cmd = [
        python_bin,
        "prose_evidence_router_compare.py",
        "--portfolio-input", args.portfolio_input,
        "--router-shadow-input", str(router_shadow),
        "--write", str(compare_json),
    ]
    run_cmd(compare_cmd, dry_run=args.dry_run)

    memory_cmd = [
        python_bin,
        "prose_evidence_router_memory_writeback.py",
        "--compare-input", str(compare_json),
        "--router-shadow-input", str(router_shadow),
        "--memory-path", args.memory_path,
    ]
    run_cmd(memory_cmd, dry_run=args.dry_run)

    promote_cmd = [
        python_bin,
        "prose_evidence_router_promote.py",
        "--portfolio-input", args.portfolio_input,
        "--router-shadow-input", str(router_shadow),
        "--router-runtime-input", str(runtime_input),
        "--write", str(advisory_input),
    ]
    run_cmd(promote_cmd, dry_run=args.dry_run)

    summary = {
        "runtime_input": str(runtime_input),
        "router_shadow": str(router_shadow),
        "compare_json": str(compare_json),
        "advisory_input": str(advisory_input),
        "promotion_count": None,
        "use_advisory_input": False,
    }

    if not args.dry_run:
        promoted = load_json(str(advisory_input))
        promotions = promoted.get("router_advisory_promotions") or []
        summary["promotion_count"] = len(promotions)
        summary["use_advisory_input"] = len(promotions) > 0

    write_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# TOOLS.md - Local Notes

Skills define _how_ tools work. This file is for _your_ specifics — the stuff that's unique to your setup.

## What Goes Here

Things like:

- Camera names and locations
- SSH hosts and aliases
- Preferred voices for TTS
- Speaker/room names
- Device nicknames
- Anything environment-specific

## Examples

```markdown
### Cameras

- living-room → Main area, 180° wide angle
- front-door → Entrance, motion-triggered

### SSH

- home-server → 192.168.1.100, user: admin

### TTS

- Preferred voice: "Nova" (warm, slightly British)
- Default speaker: Kitchen HomePod
```

## Why Separate?

Skills are shared. Your setup is yours. Keeping them apart means you can update skills without losing your notes, and share skills without leaking your infrastructure.

---

Add whatever helps you do your job. This is your cheat sheet.

---

# PROSE-RESEARCH TOOL EXTENSIONS

## Search Strategy
- Use PubMed-style queries when possible
- Use multiple query variants if results are weak
- Prefer:
  - ("psychiatry" OR "mental health") AND AI
  - disorder-specific queries (PTSD, MDD, etc.)

## Retrieval Priority
1. PMC full text
2. Publisher full text
3. Abstract + metadata

## Extraction Workflow
1. Identify study type
2. Extract:
   - sample size
   - methodology
   - key outcomes (AUC, sensitivity, etc.)
3. Note limitations explicitly

## Summarization Rules
- No generic summaries
- Always include:
  - what was done
  - what was found
  - why it matters

## Tool Usage
- Use web search for discovery
- Use structured extraction before summarization
- Use summarization only after filtering relevance


## Link Extraction Rules

Always attempt to retrieve:
1. PubMed URL
2. PMC full text link (if available)
3. Publisher link (fallback)

If full text unavailable:
- Proceed with abstract
- Do NOT discard high-quality studies

## Full Text Strategy

- Prefer PMC open-access articles
- If PubMed has "Free full text", prioritize
- Extract key sections if full text available:
  - methods
  - results
  - discussion


## File Writing Permissions

The agent is allowed to:
- Append observations to `memory/YYYY-MM-DD.md`
- Propose updates to `MEMORY.md`
- Suggest improvements to `AGENTS.md` and `TOOLS.md` (but not overwrite without confirmation)

Use file writes only when:
- a durable pattern is identified
- a repeated improvement is observed


## Memory Tool Usage

When working in Discord guild channels:
- use memory search/get tools if prior long-term context may matter
- do not assume `MEMORY.md` is already in context
- promote reusable lessons into `MEMORY.md` only after repeated success


## Workspace Layout Notes

Canonical code locations:
- `scripts/orchestration/`
- `scripts/pipeline/`
- `scripts/planner/`
- `scripts/router/`
- `scripts/reporting/`
- `scripts/diagnostics/`

Operational note:
- in the current VPS/workspace, root-level `prose_*.py` entrypoints may remain available for compatibility
- future edits should target the canonical `scripts/*` files first

Sub-agent templates and schemas:
- `.prose/templates/subagents/`

Archive/backups:
- `_archive/`


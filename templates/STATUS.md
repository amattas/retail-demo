# Development Status: [PROJECT/FEATURE NAME]

**Last Updated**: [timestamp]
**Tasks**: X completed / Y total

---

## Overview

| Task | Status | Wave | Assignee | Notes |
|------|--------|------|----------|-------|
| [task1] | Done | E | - | PR merged |
| [task2] | In Progress | D | backend-impl | Writing tests |
| [task3] | Blocked | B | - | Waiting on spec clarification |
| [task4] | Not Started | A | - | Needs context gathering |

**Legend**: Done | In Progress | Blocked | Not Started

**Waves**: A (Context) | B (Design) | C (Validation) | D (Implementation) | E (Review)

---

## Task Details

### [task1] - DONE
- **Wave**: E (Review & PR)
- **Summary**: [What was accomplished]
- **Artifacts**: `spec.md`, `architecture.md`, PR #123
- **Notes**: [Any follow-up items]

### [task2] - IN PROGRESS
- **Wave**: D (Implementation)
- **Current**: [What's being worked on]
- **Completed artifacts**: `spec.md`, `api-design.md`
- **Remaining**: `test-plan.md` implementation, docs
- **Next**: [Immediate next step]

### [task3] - BLOCKED
- **Wave**: B (Design)
- **Blocker**: [What's blocking progress]
- **Need**: [What's required to unblock]
- **Partial work**: [Any completed artifacts]

### [task4] - NOT STARTED
- **Wave**: A (Context)
- **Priority**: [High/Medium/Low]
- **Dependencies**: [Any prerequisites]

---

## Active Artifacts

Track which design/context artifacts exist and their status:

| Artifact | Status | Last Updated |
|----------|--------|--------------|
| `context/repo-map.md` | Current | [date] |
| `context/dependency-graph.md` | Stale | [date] |
| `spec.md` | Approved | [date] |
| `architecture.md` | Draft | [date] |
| `api-design.md` | Missing | - |
| `test-plan.md` | Missing | - |
| `security-requirements.md` | N/A | - |

---

## Parallel Work Plan

**Can run in parallel**:
- task1 (backend) and task2 (frontend) - different components
- Context gathering for new tasks while implementation continues

**Must be sequential**:
- Wave A -> B -> C -> D -> E for each task
- Design validation before implementation

---

## Agent Activity Log

Track which agents have been used and their outputs:

### [Date]
- `repo-scanner` -> `context/repo-map.md`
- `spec-synthesizer` -> `spec.md` (v1)
- `arch-designer` -> `architecture.md` (draft)

### [Earlier Date]
- Initial setup
- Requirements gathering

---

## Session History

### [Date]
- Completed: task1 implementation
- Progress: task2 tests 50% done
- Started: task3 design phase
- Blocked: task4 (need stakeholder input)

### [Earlier Date]
- Initial planning
- Context gathering complete

---

## Notes

[Any other observations, decisions made, or things to remember for next session]

---

## Restartability Checklist

When resuming work:

1. [ ] Read this STATUS.md to understand current state
2. [ ] Check artifact status table for stale/missing docs
3. [ ] Review blocked tasks for resolution
4. [ ] Check agent activity log for context
5. [ ] Resume from last known good state per task

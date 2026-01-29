---
name: github-workflow
description: Workflow for processing GitHub issues as development tasks with worktrees and CI validation.
---

# GitHub Issue Development Workflow

This skill defines the systematic process for working through GitHub issues.

## Priority Order

1. **Bug issues** (lowest number first)
2. **Enhancement issues** (lowest number first)
3. **Documentation issues** (lowest number first)

## Issue Classification

All issues must have at least one primary label:

| Label | When to Apply |
|-------|---------------|
| `bug` | Something is broken, incorrect, or failing |
| `enhancement` | New feature or improvement request |
| `documentation` | Docs, README, comments, guides |

## Dependency Handling

Before starting an issue, check for dependencies:

```markdown
## Parent Issue
Part of #123

## Blocked By
- #456 (must complete first)

## Dependencies
- Requires #789 to be merged
```

If dependencies exist and are open:
1. Work on the dependency first
2. Complete and merge the dependency PR
3. Return to the original issue

## Git Worktree Strategy

Each issue gets its own worktree for isolation:

```bash
# Create worktree
git worktree add ../retail-demo-issue-42 -b issue-42-fix-login origin/main

# List worktrees
git worktree list

# Remove after PR merged
git worktree remove ../retail-demo-issue-42
```

### Naming Convention
- Worktree path: `../retail-demo-issue-<number>`
- Branch name: `issue-<number>-<short-slug>`

### Parallelization Rules
- Multiple worktrees can run in parallel
- Never parallelize dependent issues
- Each worktree = independent development environment

## Pull Request Requirements

### One Issue = One PR
- Each issue gets exactly one PR
- PR title follows conventional commit format
- PR body must include `Closes #<number>` or `Fixes #<number>`

### PR Template
```markdown
## Summary
- Brief description of changes

## Test Plan
- How to verify the changes work

Closes #<issue-number>
```

### CI Validation
PRs must pass all GitHub Actions checks:

```bash
# Watch CI status
gh pr checks <pr-number> --watch

# View failed run logs
gh run view <run-id> --log-failed
```

If CI fails:
1. Analyze the failure
2. Fix in the worktree
3. Commit and push
4. Wait for CI to re-run
5. Repeat until green

## Completion Criteria

An issue is complete when:
1. PR is created and linked to issue
2. All CI checks pass
3. Code review approved (if required)
4. PR is merged
5. Issue is automatically closed

## Commands Reference

| Command | Purpose |
|---------|---------|
| `/work` | Start the full workflow |
| `/work classify` | Only classify unlabeled issues |
| `/work next` | Show next issue to work on |
| `/work 42` | Work on specific issue #42 |
| `/work status` | Show active worktrees |

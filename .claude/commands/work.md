---
description: Fetch GitHub issues and work through them systematically (bugs first, then enhancements)
---
$ARGUMENTS

# GitHub Issue Development Workflow

Execute the following workflow to process GitHub issues systematically.

## Phase 1: Issue Classification

1. Fetch all open issues: `gh issue list --state open --json number,title,labels,body --limit 100`
2. Check each issue for proper labeling:
   - Must have at least one of: `bug`, `enhancement`, `documentation`
   - If unlabeled, analyze the issue body and classify:
     - "Something isn't working" → `bug`
     - "New feature or request" → `enhancement`
     - "Documentation changes" → `documentation`
   - Apply missing labels: `gh issue edit <number> --add-label <label>`
3. Report classification summary

## Phase 2: Issue Selection

1. Get prioritized issue list:
   - First: All `bug` issues, sorted by number ascending (lowest first)
   - Then: All `enhancement` issues, sorted by number ascending
2. For the selected issue, check for dependencies:
   - Look for "blocked by", "depends on", "parent issue", or `blockedBy` references
   - If dependencies exist and are open, work on those first
3. Announce which issue will be worked on

## Phase 3: Development Setup (Git Worktree)

1. Ensure main branch is up to date: `git fetch origin && git checkout main && git pull`
2. Create a worktree for the issue:
   ```
   git worktree add ../retail-demo-issue-<number> -b issue-<number>-<slug>
   ```
   Where `<slug>` is a short kebab-case summary of the issue title
3. Change to the worktree directory
4. Confirm setup is ready

## Phase 4: Implementation

1. Read the full issue details: `gh issue view <number>`
2. Analyze the issue requirements and acceptance criteria
3. Implement the fix/feature following project conventions:
   - Use existing patterns from the codebase
   - Follow CLAUDE.md guidelines
   - Write tests for changes
4. Run local verification:
   - `ruff check datagen/` - Linting
   - `ruff format datagen/` - Formatting
   - `mypy datagen/src/` - Type checking
   - `pytest datagen/tests/` - Tests

## Phase 5: Pull Request

1. Stage and commit changes with conventional commit format:
   - `fix(<scope>): <description>` for bugs
   - `feat(<scope>): <description>` for enhancements
   - Reference the issue: `Fixes #<number>` or `Closes #<number>`
2. Push the branch: `git push -u origin issue-<number>-<slug>`
3. Create PR linked to issue:
   ```
   gh pr create --title "<type>(<scope>): <summary>" \
     --body "## Summary\n<description>\n\n## Test Plan\n<how to verify>\n\nCloses #<number>" \
     --base main
   ```

## Phase 6: CI Validation

1. Wait for GitHub Actions to complete: `gh pr checks <pr-number> --watch`
2. If checks fail:
   - Review the failure: `gh run view <run-id> --log-failed`
   - Fix the issues in the worktree
   - Commit and push fixes
   - Repeat until all checks pass
3. Once all checks pass, report success

## Phase 7: Cleanup

1. Return to main worktree: `cd $(git worktree list | head -1 | awk '{print $1}')`
2. Optionally remove the worktree after PR is merged:
   ```
   git worktree remove ../retail-demo-issue-<number>
   ```

## Parallelization

When working on multiple issues in parallel:
- Each issue gets its own worktree
- Use separate terminal sessions or background processes
- Never work on dependent issues in parallel
- Coordinate commits to avoid merge conflicts

## Arguments

If $ARGUMENTS contains:
- A number: Work on that specific issue
- "classify": Only run Phase 1 (classification)
- "next": Select and announce the next issue without starting work
- "status": Show current worktrees and their issue status

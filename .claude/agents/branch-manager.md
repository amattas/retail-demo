---
name: branch-manager
description: USE when starting new work. Creates a feature branch and prepares the workspace.
tools: Bash, Read
model: haiku
skills: commit-conventions
---
Inputs:
- `spec.md` and current branch state
- Commit-conventions skill for naming patterns (if desired)

Tasks:
- Create a feature branch with a name derived from the spec (slugified).
- Ensure the working tree is clean before branching.
- Optionally scaffold minimal required files or directories.

Output: branch name and summary of actions taken.

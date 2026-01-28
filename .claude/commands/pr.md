---
description: Create a pull request with full context
---
$ARGUMENTS

1. Determine the base branch (usually `main`)
2. Run `git log main..HEAD --oneline` to see all commits on this branch
3. Run `git diff main...HEAD --stat` to see all file changes
4. Generate a PR following this format:
   - **Title**: Concise summary of the change
   - **Summary**: 1-3 bullet points explaining what changed and why
   - **Test plan**: How to verify the changes work
5. Create the PR using `gh pr create`
6. Return the PR URL

If $ARGUMENTS contains a target branch, use that instead of main.

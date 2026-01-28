---
description: Stage, review, and commit changes with conventional commit format
---
$ARGUMENTS

1. Run `git status` and `git diff --stat` to see all changes
2. Analyze the changes and determine the appropriate conventional commit type:
   - `feat` - new user-facing feature
   - `fix` - bug fix
   - `refactor` - structural code changes without behavior change
   - `chore` - tooling, CI, non-runtime code
   - `docs` - documentation-only changes
   - `perf` - performance improvements
   - `test` - adding or adjusting tests
3. Suggest a commit message following the format: `<type>(<scope>): <short summary>`
4. Ask for confirmation or edits before committing
5. Stage only relevant files (never use `git add -A` or `git add .`)
6. Create the commit with the approved message

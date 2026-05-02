---
description: Debug a reproducible failing test, API error, or worker/dispatcher issue using reproduce-isolate-fix-verify.
---

1. Reproduce the issue with the exact command or curl request.
2. Capture the stack trace, log output, or failing behavior.
3. Identify the smallest set of files involved.
4. Add or update a failing test if practical.
5. Propose the smallest safe fix.
6. Apply the fix.
7. Re-run targeted verification first.
8. Summarize root cause, changed files, verification result, and remaining risk.
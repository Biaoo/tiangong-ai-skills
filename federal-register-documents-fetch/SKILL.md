---
name: federal-register-documents-fetch
description: Legacy compatibility alias for older prompts, source selections, or archived runs that still reference federal-register-documents-fetch. Use only when an existing workflow explicitly requires this deprecated skill name; otherwise prefer $federal-register-doc-search for new Federal Register work.
---

# Legacy Alias

- This skill name is retained only for backward compatibility with older artifacts and prompts.
- New orchestration, source policy, and source selection should use `$federal-register-doc-search`.
- Historical runs that already emitted `federal-register-documents-fetch` can still be normalized because `$eco-council-normalize` accepts both names.

## Preferred Replacement

Use `$federal-register-doc-search` for:

- official U.S. rulemaking or notice discovery
- publication-window Federal Register searches
- new eco-council sociologist source selections

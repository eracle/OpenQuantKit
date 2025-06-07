# Contribution Guidelines for LLM Coders

These instructions apply to the entire repository.

## Commit Guidelines
- Work directly on the default branch.
- Keep commits focused and descriptive.

## Code Style
- If you modify any Python files, format only the changed files using
  `black --line-length 120`.
- After formatting, run `black --check` on the changed files to verify.

## Programmatic Checks
- There are currently no automated tests.
- If Python files were changed, ensure the formatter check passes before committing.

## Pull Request Instructions
- Summaries should describe the key changes and mention the files touched.
- Include a **Testing** section summarizing any commands run.
- If any checks cannot be run due to environment limitations, note that in the **Testing** section.

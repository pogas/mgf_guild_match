# AGENTS.md

## Scope

This file applies to everything under `mgf_guild_report/`.

## Design source of truth

- Always read `DESIGN.md` before changing UI, layout, copy tone, or component styling.
- Treat `DESIGN.md` as the visual/design-system contract for this project.
- Preserve the project’s MapleStory-like, cozy, game-adjacent report aesthetic.
- Do **not** replace the current design with generic SaaS, dashboard, terminal, or startup landing-page aesthetics.

## UI implementation rules

- Reuse existing component vocabulary whenever possible:
  - `hero`, `mode-tabs`, `section-tabs`
  - `summary-card`, `auto-summary-card`
  - `guild-card`, `detail-compare-card`
  - `modal-backdrop`, `modal-box`, `simulation-modal-box`
  - `member-table`, `simulation-table`, `tobeol-ranking-table`
- Prefer extending existing tokens/classes over inventing unrelated ones.
- Keep rounded surfaces, soft shadows, warm cream backgrounds, and muted helper copy.
- Maintain current responsive behavior around the existing breakpoints (`980px`, `720px`).

## Report generator workflow

- Most UI is generated from `mgf_guild_export.py`.
- When changing generator output, regenerate the affected report files and verify the output.
- For report-only copy/styling changes, keep refactors small and behavior-preserving.

## Content and interaction rules

- Prioritize readability over spectacle.
- Keep summary sections high-signal and compact.
- Tables may be data-dense, but surrounding layout should remain breathable.
- Mobile should prefer stacked cards or simplified layouts over forcing wide-table usage.

## Git / change management

- Use focused commits with Korean semantic commit messages when committing from this project.
- Avoid bundling unrelated generated outputs with a small source-only change.

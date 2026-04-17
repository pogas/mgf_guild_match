# MGF Guild Report DESIGN.md

## 1. Purpose

This project is a **static MapleStory-like guild report UI** for league, training, and guild/tobeol reporting.

The design must feel:

- cozy and game-adjacent
- readable for long report sessions
- soft and warm rather than harsh or corporate
- data-rich without feeling dense or stressful

This is **not** a SaaS dashboard, startup landing page, or terminal UI.

Primary goals:

- make guild comparison easy
- make rankings and history readable at a glance
- preserve a polished Maple-style fantasy mood
- keep mobile usage practical for long tables and simulation views

---

## 2. Visual Theme & Atmosphere

### Core mood

- warm cream paper surfaces
- pastel sky gradients
- soft orange and moss-green accents
- rounded fantasy UI with gentle shadows
- polished but friendly “game event report” feeling

### Density

- medium density
- summary first, detail second
- tables can be data-heavy, but surrounding layout must stay breathable

### Design philosophy

- one screen should never feel overloaded
- primary hierarchy comes from spacing, rounded surfaces, and accent color
- use visual softness to offset analytical density
- cards and pills should feel collectible/game-like, not enterprise-like

---

## 3. Color Palette & Roles

Use these semantic tokens exactly.

| Token | Value | Role |
|---|---:|---|
| `--bg` | `#f7f3ec` | global warm background |
| `--bg-alt` | `#fffaf3` | lighter alternate surface |
| `--sky-top` | `#e4f3ff` | upper gradient sky wash |
| `--sky-bottom` | `#f9f3e6` | lower gradient warmth |
| `--cloud` | `rgba(255, 255, 255, 0.76)` | decorative cloud blobs |
| `--panel` | `rgba(255, 252, 247, 0.92)` | standard panel/card surface |
| `--panel-strong` | `rgba(250, 244, 236, 0.96)` | stronger elevated surface |
| `--line` | `rgba(110, 84, 60, 0.12)` | subtle borders/dividers |
| `--text` | `#2e241d` | primary text |
| `--muted` | `#7a6658` | secondary/help text |
| `--accent` | `#d47d5a` | warm primary accent |
| `--accent-2` | `#88b17c` | supportive green accent |
| `--accent-3` | `#ad6540` | stronger orange-brown emphasis |
| `--shadow` | `0 18px 44px rgba(78, 58, 42, 0.12)` | standard card shadow |
| `--radius` | `22px` | default card radius |

### Color usage rules

- `--text` for all primary body and table text
- `--muted` for explanation, helper copy, trend hints, table sublabels
- `--accent-3` for emphasis labels, active highlights, important metric text
- `--accent-2` for positive/supportive visual variation, not as the dominant global accent
- use `--line` for almost all borders; avoid dark hard outlines

### Do not

- do not introduce neon colors
- do not switch to pure black/white product branding
- do not use saturated red/green dashboards unless the state is critical

---

## 4. Typography Rules

### Font family

Primary display and UI font:

```txt
"Maplestory", "Apple SD Gothic Neo", "Malgun Gothic", sans-serif
```

Font faces are mapped through:

- `light` → `Maplestory Light.ttf`
- `bold` → `Maplestory Bold.ttf`

### Tone

- headings should feel game-like and collectible
- body text should remain easy to scan and never overly decorative
- avoid sleek startup typography or ultra-thin editorial luxury styles

### Hierarchy

| Element | Guidance |
|---|---|
| Hero title | large, bold, clamp-based responsive size |
| Section title | medium-large, strong but not loud |
| Card metric value | bold, compact, easy to scan |
| Eyebrow label | uppercase feel, pill-like, decorative but readable |
| Body copy | calm, medium size, generous line-height |
| Caption / helper | smaller, muted |
| Table value | crisp and compact |
| Numeric metric | prioritize clarity and alignment |

### Existing scale references

- hero title uses large clamp-based sizes
- summary values use compact large numeric emphasis
- helper copy is often 11px–13px equivalent and muted

### Do not

- do not use overly tiny dense captions for important information
- do not mix multiple decorative fonts
- do not make headings excessively long or marketing-heavy

---

## 5. Layout Principles

### Global structure

- centered report container
- wide desktop layout with controlled max width
- hero/header first
- summary and comparison modules second
- detail tables and modals afterward

### Spacing behavior

- prefer soft separation through spacing before using borders
- keep vertical rhythm consistent between sections
- give cards enough padding to feel breathable
- use tighter spacing only inside table/grid-heavy sections

### Width and composition

- top-level page should feel like a premium game report board
- major sections should stack naturally on smaller screens
- horizontal scroll is acceptable for comparison card rails and large tables only when necessary

### Surface hierarchy

1. page background gradient
2. hero surface
3. standard card/panel surface
4. inner chips / pills / mini cards

---

## 6. Component Stylings

Document components by intent, not only by class name.

### Hero

Classes:

- `hero`
- `hero-copy`
- `hero-title-row`
- `hero-title-mark`
- `eyebrow`
- `lead`

Rules:

- should feel like an event banner, not a SaaS masthead
- use warm surface gradients and soft atmospheric decoration
- title + guild mark pairing is important

### Mode tabs

Classes:

- `mode-tabs`
- `mode-tab`

Rules:

- compact pill navigation
- active tab should feel gently highlighted, not aggressively selected
- must remain easy to tap on mobile

### Section tabs / major actions

Classes:

- `section-tabs`

Rules:

- stronger visual weight than mode tabs
- should feel like feature entrances into simulations and analysis

### Summary cards

Classes:

- `summary-card`
- `auto-summary-card`
- `auto-summary-grid`
- `summary-label`
- `summary-value`
- `summary-help`

Rules:

- short headline
- one strong metric
- one line of supporting explanation
- never overload with multiple competing stats

### Comparison cards

Classes:

- `guild-card`
- `guild-card-top`
- `guild-card-mark`
- `rank-pill`
- `rank-badge`
- `power-meter`
- `analysis-chip`

Rules:

- this is the “collectible card” layer of the UI
- use rounded cards, compact chips, and strong numeric ranking cues
- comparison cards should feel swipeable and lightweight on mobile

### Detail comparison cards

Classes:

- `detail-compare-card`
- `detail-compare-table`

Rules:

- use for side-by-side factual inspection
- less decorative than top-level comparison cards

### Modals

Classes:

- `modal-backdrop`
- `modal-box`
- `modal-close`
- `simulation-modal-box`

Rules:

- modals should feel like opening a report chapter
- use stronger shadow and rounded corners
- content inside modals can be dense, but entry surface must still feel premium

### Simulation ranking cards

Classes:

- `simulation-rank-card`
- `simulation-rank-badge`
- `simulation-rank-score`
- `simulation-rank-summary`

Rules:

- emphasize rank and score immediately
- details are collapsible
- keep summary visible even before expansion

### Simulation member cards

Classes:

- `simulation-member-card`
- `simulation-member-rank`
- `simulation-member-score`
- `simulation-member-power`

Rules:

- used as mobile replacement for large tables
- rank, name, guild, and key metric must be visible at a glance

### Tables

Classes:

- `table-wrap`
- `table-toolbar`
- `member-table`
- `simulation-table`

Rules:

- tables are factual surfaces, not decorative surfaces
- maintain generous row readability
- sticky headers are acceptable
- sort/filter affordances should be obvious but subtle

### Tobeol ranking table

Classes:

- `tobeol-ranking-tabs`
- `tobeol-ranking-tab`
- `tobeol-ranking-table`
- `tobeol-rank-chip`
- `tobeol-rank-chip-muted`
- `tobeol-unranked-row`

Rules:

- ranked and unranked states must both be understandable
- `미등재` rows should look secondary but still belong to the same guild list
- summary should show ranked count vs total count clearly

### Badges and chips

Classes:

- `badge`
- `badge-master`
- `analysis-chip`
- `rank-pill`

Rules:

- always rounded pill form
- concise text only
- used for state, identity, or short metadata

---

## 7. Depth & Elevation

### Standard depth

- use `--shadow` for cards and panels
- borders should remain subtle and warm

### Stronger depth

- modals and primary hero sections can use deeper shadow and larger radius

### Flat areas

- tables and inner content zones can be flatter for readability

### Do not

- do not stack heavy shadows on every nested component
- do not use glossy or glassmorphism-heavy effects
- do not create high-contrast floating dashboard tiles everywhere

---

## 8. Responsive Behavior

### Breakpoints

- primary compression around `980px`
- mobile-focused layout changes around `720px`

### At medium widths

- multi-column grids collapse to single column where needed
- toolbars and section headers stack vertically

### At mobile widths

- hero padding and corner radius reduce
- large tables may hide in favor of mobile cards
- card rails remain swipe-friendly
- filters/search should stretch to full width
- comparison and analytics layouts become single-column

### Mobile priorities

1. key summary cards
2. guild comparison
3. mobile simulation cards
4. reduced-complexity details

### Touch targets

- tabs, toggles, pills, and modal controls must remain easy to tap

---

## 9. Interaction & State Rules

### Modal behavior

- open through `data-modal`
- close through close button, backdrop click, and `Esc`

### Expand/collapse

- simulation detail blocks and analytics modules can collapse
- collapsed state should still show a useful summary

### Table interactions

- sorting through table headers
- filtering through guild filters and search inputs
- use muted helper labels rather than loud control chrome

### Scroll behavior

- horizontal drag-scroll is acceptable for compare card rails
- avoid requiring horizontal scroll for the main reading flow unless unavoidable

---

## 10. Do’s and Don’ts

### Do

- keep the Maple-like warm fantasy tone
- prioritize readability over spectacle
- make rank and score immediately scannable
- use cards to group related metrics cleanly
- keep secondary explanation text calm and muted
- preserve soft gradients, rounded shapes, and pill chips

### Don’t

- do not turn the project into a startup landing page
- do not use stark black-and-white enterprise minimalism
- do not overload a single screen with too many equal-weight modules
- do not use giant promotional hero copy or marketing CTAs
- do not introduce unrelated brand aesthetics from external references
- do not make mobile depend on wide tables where card alternatives exist

---

## 11. AI Agent Prompt Guide

When generating or modifying UI for this project, prefer the following:

- Use a cozy MapleStory-like report aesthetic.
- Preserve warm cream surfaces and pastel sky gradients.
- Favor rounded panels, pill chips, and soft shadows.
- Keep summaries short and high-signal.
- Treat comparison cards like collectible game info cards.
- Keep tables factual and readable, not flashy.
- On mobile, replace wide table experiences with stacked cards when possible.
- Use muted helper copy for explanations and strong accent tones only for emphasis.
- Preserve the existing class/token vocabulary when extending current UI.
- Avoid importing another product’s brand identity directly.

---

## 12. Component Vocabulary To Preserve

Prefer extending the existing system instead of inventing unrelated names.

Key classes and patterns already in use:

- `hero`, `hero-copy`, `hero-title-row`, `hero-title-mark`
- `mode-tabs`, `mode-tab`, `section-tabs`
- `summary-card`, `auto-summary-card`, `auto-summary-grid`
- `guild-card`, `detail-compare-card`
- `modal-backdrop`, `modal-box`, `simulation-modal-box`
- `simulation-rank-card`, `simulation-member-card`
- `member-table`, `simulation-table`
- `tobeol-ranking-table`, `tobeol-rank-chip`
- `badge`, `badge-master`, `analysis-chip`
- `analytics-grid`, `analytics-module`, `analytics-chapter`

If new UI is introduced, it should feel like a natural sibling of these components.

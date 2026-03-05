<p align="center">
  <img src="visualize/oscp-logo.svg" height="72" alt="OSCP+">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="https://github.com/0XBUGATTI.png" height="72" width="72" style="border-radius:50%" alt="0XBUGATTI">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="https://github.com/anthropics.png" height="72" width="72" alt="Anthropic Claude">
</p>

<h1 align="center">OSCP+ Repos</h1>

<p align="center">
  <em>Every repo on GitHub that mentions OSCP — hunted, cloned, read by AI, and judged by a human.</em>
</p>

<p align="center">
  <a href="https://oscp-repos.vercel.app"><img src="https://img.shields.io/badge/Live%20App-Vercel-black?style=flat-square&logo=vercel" alt="Vercel"></a>
  &nbsp;
  <img src="https://img.shields.io/badge/399%20repos-curated-E66124?style=flat-square" alt="399 repos">
  &nbsp;
  <img src="https://img.shields.io/badge/AI-claude--sonnet--4--6-blueviolet?style=flat-square&logo=anthropic" alt="Claude">
  &nbsp;
  <img src="https://img.shields.io/badge/DB-SQLite-003B57?style=flat-square&logo=sqlite" alt="SQLite">
</p>
![poc](poc.png)

---

## The Story

I was prepping for OSCP and wanted a single place to browse every relevant GitHub repo — notes, tools, writeups, cheatsheet.

So I built a pipeline that did the work for me.

**Step 1 — Hunt.**
GitHub Search API, paginated across every repo created or pushed after mid-2025 with `OSCP` in its name or description. ~500 raw results.

**Step 2 — Clone.**
Shallow-cloned (`--depth 1`) each repo locally. Repos too large (>500MB) fell back to the GitHub Contents API.

**Step 3 — Extract.**
Walked every file tree, pulled READMEs. Stored everything as Base64 in SQLite — no flat files, no exports, just one clean `.db`.

**Step 4 — Ask Claude.**
Fed each repo's description + file paths + README preview to `claude-sonnet-4-6`. Asked it to assign one of five categories:

| Category | Meaning |
|---|---|
| `OSCP+` | Notes / cheatsheets exclusively for OSCP/PEN-200 |
| `Tools` | Standalone security tools and scripts |
| `Writeups` | HTB / THM / CTF walkthroughs |
| `Sensitive` | OffSec exam content — Proving Grounds, exam reports |
| `Generic` | Multi-cert notes (OSCP + HTB + CRTP + ...) |

**Step 5 — Review.**
Manually went through every `None` result. Deleted 60+ repos that had nothing to do with offensive security — OS course projects in Chinese, Open Sound Control libraries, EV charging protocols, Oscar award prediction websites. You know how it is.

**Result: 399 clean repos.**

---

## The Web App

Built a dark-mode dashboard on top of the SQLite DB — no data exports, no JSON files, the Node.js server queries the DB directly at request time.

**Features**
- Filter by category · search by name / description / language
- Sort by stars, size, last push, AI confidence
- Min-stars filter · per-page control
- Drag-to-resize columns
- Click any row → side panel with file tree, file list search, rendered README, AI reasoning
- Export current view as CSV

**Stack:** Node.js · Express · better-sqlite3 · Vanilla JS · Space Grotesk

---

## Run Locally

```bash
git clone https://github.com/0xbugatti/oscp-collector
cd oscp-collector
npm install
node server.js
# → http://127.0.0.1:8787
```

> The `oscp.db` file ships with the repo. No pipeline run needed to use the dashboard.

---

## Pipeline (optional — to refresh the data)

```bash
pip install -r requirements.txt
export GITHUB_TOKEN=...
export ANTHROPIC_API_KEY=...
python main.py          # fetch → clone → extract → categorize → export
python main.py status   # check progress at any point
```

---

## Deployed

**[oscpdb.vercel.app](https://oscp-repos.vercel.app)** ← live app

---

*Built while studying for OSCP. Try harder.*

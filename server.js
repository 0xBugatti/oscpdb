/**
 * server.js — OSCP Repos dashboard server
 * Express + better-sqlite3, no data.json needed
 *
 * Usage:  node server.js          (port 8787)
 *         node server.js 9000     (custom port)
 */
const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const PORT = parseInt(process.argv[2]) || 8787;

// Pick best available database
const DB_PATH = (() => {
  for (const name of ['oscp.db', 'oscp_repos.db']) {
    const p = path.join(__dirname, name);
    if (fs.existsSync(p)) return p;
  }
  throw new Error('No database found (oscp.db or oscp_repos.db)');
})();

const db       = new Database(DB_PATH, { readonly: true });
const DB_NAME  = path.basename(DB_PATH);
const VIS_DIR  = path.join(__dirname, 'visualize');

// Whitelisted sort expressions
const SORT_MAP = {
  stars:   'CAST(stars   AS INTEGER)',
  size:    'CAST(size_kb AS INTEGER)',
  pushed:  'pushed_at',
  created: 'created_at',
  name:    'LOWER(name)',
  conf:    'CAST(category_confidence AS REAL)',
  cat:     'category',
};

const LIST_COLS = [
  'id','name','full_name','description','url',
  'size_kb','stars','forks','language','topics',
  'created_at','pushed_at',
  'category','category_confidence','category_reasoning',
].join(', ');

const DETAIL_COLS = LIST_COLS + ', file_structure_b64, file_names_b64, readme_b64';

function parseTopics(val) {
  if (!val) return [];
  try   { const p = JSON.parse(val); return Array.isArray(p) ? p : []; }
  catch { return String(val).split(',').map(s => s.trim()).filter(Boolean); }
}

function repoRow(r) {
  return { ...r, topics: parseTopics(r.topics) };
}

// ── Routes ────────────────────────────────────────────────────────────────────

const app = express();

app.get('/api/stats', (_req, res) => {
  const total    = db.prepare('SELECT COUNT(*) n FROM repos').get().n;
  const cats     = db.prepare('SELECT category, COUNT(*) cnt FROM repos GROUP BY category').all();
  const dates    = db.prepare('SELECT MIN(pushed_at) lo, MAX(pushed_at) hi FROM repos').get();
  const langs    = db.prepare(
    "SELECT language, COUNT(*) cnt FROM repos WHERE language IS NOT NULL AND language != '' GROUP BY language ORDER BY cnt DESC LIMIT 8"
  ).all();
  const result   = { total, db: DB_NAME, dates, langs };
  cats.forEach(r => { result[r.category || 'None'] = r.cnt; });
  res.json(result);
});

app.get('/api/repos', (req, res) => {
  const q        = String(req.query.q        || '').trim();
  const cat      = String(req.query.cat      || '').trim();
  const sortKey  = req.query.sort in SORT_MAP ? req.query.sort : 'stars';
  const dir      = req.query.dir === 'asc' ? 'ASC' : 'DESC';
  const page     = Math.max(1, parseInt(req.query.page) || 1);
  const per      = Math.min(2000, Math.max(10, parseInt(req.query.per) || 50));
  const minStars = Math.max(0, parseInt(req.query.minStars) || 0);
  const minSize  = Math.max(0, parseInt(req.query.minSize)  || 0);

  const conds = [], params = [];

  if (cat && cat !== 'all') { conds.push('category = ?');                   params.push(cat); }
  if (q)                    { conds.push('(name LIKE ? OR full_name LIKE ? OR description LIKE ? OR language LIKE ?)');
                               const like = `%${q}%`; params.push(like,like,like,like); }
  if (minStars > 0)         { conds.push('CAST(stars   AS INTEGER) >= ?'); params.push(minStars); }
  if (minSize  > 0)         { conds.push('CAST(size_kb AS INTEGER) >= ?'); params.push(minSize);  }

  const where = conds.length ? 'WHERE ' + conds.join(' AND ') : '';
  const expr  = SORT_MAP[sortKey];

  const total = db.prepare(`SELECT COUNT(*) n FROM repos ${where}`).get(...params).n;
  const rows  = db.prepare(
    `SELECT ${LIST_COLS} FROM repos ${where} ORDER BY ${expr} ${dir} NULLS LAST LIMIT ? OFFSET ?`
  ).all(...params, per, (page - 1) * per);

  res.json({
    repos: rows.map(repoRow),
    total, page, per,
    pages: Math.max(1, Math.ceil(total / per)),
  });
});

app.get('/api/repo/:id', (req, res) => {
  const row = db.prepare(`SELECT ${DETAIL_COLS} FROM repos WHERE id = ?`).get(req.params.id);
  if (!row) return res.status(404).json({ error: 'not found' });
  res.json(repoRow(row));
});

app.use(express.static(VIS_DIR));

// ── Start ─────────────────────────────────────────────────────────────────────

// Export for Vercel serverless; listen only when run directly
if (require.main === module) {
  app.listen(PORT, '127.0.0.1', () => {
    console.log(`\n  OSCP Repos Dashboard`);
    console.log(`  DB   : ${DB_PATH}`);
    console.log(`  URL  : http://127.0.0.1:${PORT}`);
    console.log(`  Ctrl+C to stop\n`);

    setTimeout(() => {
      const open = process.platform === 'win32' ? 'start' :
                   process.platform === 'darwin' ? 'open' : 'xdg-open';
      require('child_process').exec(`${open} http://127.0.0.1:${PORT}`);
    }, 400);
  });
}

module.exports = app;

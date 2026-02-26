import os
import json
import time
import hashlib
import threading
import traceback
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file, send_from_directory
import sqlite3
import openpyxl
from openpyxl import Workbook
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import anthropic
import re
import io

app = Flask(__name__, static_folder='../frontend', static_url_path='')

# ── Storage paths ──
# On Railway: mount a Volume at /data  (Settings → Volumes → Mount path: /data)
# Locally: uses ./data relative to project root
DATA_DIR = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', os.path.join(os.path.dirname(__file__), '..', 'data'))
DB_PATH   = os.path.join(DATA_DIR, 'db.sqlite')
UPLOAD_DIR = os.path.join(DATA_DIR, 'uploads')
EXPORT_DIR = os.path.join(DATA_DIR, 'exports')

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

anthropic_client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            status TEXT DEFAULT 'pending',
            total_rows INTEGER DEFAULT 0,
            processed_rows INTEGER DEFAULT 0,
            created_at TEXT,
            completed_at TEXT,
            config TEXT
        );
        CREATE TABLE IF NOT EXISTS lawyer_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            row_index INTEGER,
            raw_data TEXT,
            site_final TEXT,
            site_status TEXT,
            primary_area_1 TEXT,
            primary_area_2 TEXT,
            primary_area_3 TEXT,
            secondary_areas TEXT,
            confidence INTEGER,
            recommendation TEXT,
            recommendation_reason TEXT,
            evidence_1 TEXT,
            evidence_2 TEXT,
            facebook_found TEXT,
            checked_at TEXT,
            status TEXT DEFAULT 'pending',
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS site_cache (
            url TEXT PRIMARY KEY,
            site_final TEXT,
            classification TEXT,
            content_hash TEXT,
            last_checked TEXT
        );
    ''')
    conn.commit()
    conn.close()

init_db()

# ─────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; LegalResearcher/1.0)',
    'Accept-Language': 'he,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def normalize_url(url):
    if not url:
        return None
    url = url.strip()
    if not url.startswith('http'):
        url = 'https://' + url
    return url

def fetch_url(url, timeout=10):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.url, r.text
    except Exception:
        pass
    # Try http fallback
    if url.startswith('https://'):
        try:
            r = requests.get(url.replace('https://', 'http://'), headers=HEADERS, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.url, r.text
        except Exception:
            pass
    return None, None

def crawl_site(start_url, max_depth=2, max_pages=15):
    """Controlled crawl of a site, returning list of (url, text) tuples."""
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc

    visited = set()
    queue = [(start_url, 0)]
    pages = []

    PRIORITY_KEYWORDS = ['תחומי', 'שירותים', 'practice', 'services', 'about', 'אודות', 'expertise', 'areas']

    while queue and len(pages) < max_pages:
        url, depth = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        final_url, html = fetch_url(url)
        if not html:
            continue

        soup = BeautifulSoup(html, 'html.parser')
        text = extract_clean_text(soup)
        pages.append((url, text))

        if depth < max_depth:
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                full = urljoin(url, href)
                p = urlparse(full)
                if p.netloc == base_domain and full not in visited:
                    # Prioritize relevant pages
                    score = sum(1 for kw in PRIORITY_KEYWORDS if kw.lower() in full.lower() or kw.lower() in a.get_text().lower())
                    links.append((score, full))
            links.sort(reverse=True)
            for _, link in links[:20]:
                queue.append((link, depth + 1))

    return pages

def extract_clean_text(soup):
    # Remove nav/footer noise
    for tag in soup.find_all(['nav', 'footer', 'script', 'style', 'header']):
        tag.decompose()

    texts = []
    for h in soup.find_all(['h1', 'h2', 'h3']):
        t = h.get_text(' ', strip=True)
        if t:
            texts.append(f"[HEADING] {t}")

    for ul in soup.find_all(['ul', 'ol']):
        for li in ul.find_all('li'):
            t = li.get_text(' ', strip=True)
            if t and len(t) < 200:
                texts.append(f"[ITEM] {t}")

    for p in soup.find_all('p'):
        t = p.get_text(' ', strip=True)
        if t and len(t) > 30:
            texts.append(t)

    return '\n'.join(texts)[:8000]

def web_search_for_site(lawyer_name, city):
    """Use Anthropic's web_search tool to find the lawyer's website."""
    try:
        query = f"{lawyer_name} עורך דין {city}"
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": f"Find the official website URL for this Israeli lawyer/law firm: {query}. Return only the URL, nothing else."}]
        )
        for block in response.content:
            if hasattr(block, 'text'):
                urls = re.findall(r'https?://[^\s\'"<>]+', block.text)
                for url in urls:
                    p = urlparse(url)
                    if p.netloc and 'facebook' not in p.netloc and 'google' not in p.netloc:
                        return url
    except Exception as e:
        print(f"Web search error: {e}")
    return None

def web_search_for_facebook(lawyer_name):
    """Search for Facebook page URL only."""
    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": f"Find the Facebook page URL for Israeli lawyer: {lawyer_name}. Return only the facebook.com URL if found, nothing else."}]
        )
        for block in response.content:
            if hasattr(block, 'text'):
                urls = re.findall(r'https?://(?:www\.)?facebook\.com/[^\s\'"<>]+', block.text)
                if urls:
                    return urls[0]
    except Exception as e:
        print(f"FB search error: {e}")
    return None

def classify_practice_areas(text_corpus, lawyer_name):
    """Use Claude to classify practice areas from website text."""
    if not text_corpus.strip():
        return None

    prompt = f"""You are analyzing the website of an Israeli lawyer or law firm named "{lawyer_name}".

Website content:
{text_corpus[:6000]}

Analyze the content and identify their practice areas. Return ONLY valid JSON in this exact format:
{{
  "primary_practice_areas": ["area1", "area2"],
  "secondary_practice_areas": ["area3", "area4"],
  "confidence": 75,
  "evidence": ["snippet1", "snippet2"]
}}

Rules:
- primary_practice_areas: max 3 items, main focus areas
- secondary_practice_areas: max 5 items, mentioned but less prominent
- confidence: 0-100 based on clarity of evidence
- evidence: 2-3 short snippets from the text (in Hebrew or English as found)
- Use clear Hebrew or English practice area names like: נדל"ן, משפחה, פלילי, עבודה, מסחרי, נזיקין, ירושה, הגירה, מקרקעין, תאגידים, etc."""

    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text
        # Extract JSON
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        print(f"Classification error: {e}")
    return None

def apply_business_rules(classification, config):
    """Determine YES/NO/MAYBE based on configured rules."""
    if not classification:
        return "MAYBE", "Could not classify - no website content"

    target = [a.strip().lower() for a in config.get('target_areas', [])]
    excluded = [a.strip().lower() for a in config.get('excluded_areas', [])]

    all_areas = [a.lower() for a in
                 classification.get('primary_practice_areas', []) +
                 classification.get('secondary_practice_areas', [])]

    # Check exclusions first
    for area in all_areas:
        for ex in excluded:
            if ex and ex in area:
                return "NO", f"Excluded area found: {area}"

    # Check targets
    for area in all_areas:
        for tgt in target:
            if tgt and tgt in area:
                return "YES", f"Target area matched: {area}"

    if not target:
        return "MAYBE", "No target areas configured"

    return "MAYBE", "No matching target areas found"

# ─────────────────────────────────────────
# Background processing
# ─────────────────────────────────────────

def process_row(job_id, row_id, raw_data, config):
    conn = get_db()
    try:
        data = json.loads(raw_data)
        lawyer_name = data.get('שם בית העסק', '')
        site_input = data.get('אתר בית', '')
        facebook_input = data.get('עמוד פייסבוק', '')
        city = data.get('ישוב', '')

        # Step 2: Determine final website
        site_final = None
        site_status = None

        if site_input:
            url = normalize_url(site_input)
            final_url, html = fetch_url(url)
            if final_url:
                site_final = final_url
            else:
                site_status = "FETCH_FAILED"

        if not site_final:
            found = web_search_for_site(lawyer_name, city)
            if found:
                final_url, html = fetch_url(found)
                if final_url:
                    site_final = final_url
                else:
                    site_status = "SEARCH_FOUND_BUT_FETCH_FAILED"

        if not site_final:
            site_status = "NO_SITE"

        # Check cache
        classification = None
        if site_final:
            cache_row = conn.execute('SELECT * FROM site_cache WHERE url=?', (site_final,)).fetchone()
            if cache_row:
                last_checked = datetime.fromisoformat(cache_row['last_checked'])
                if datetime.now() - last_checked < timedelta(days=7):
                    classification = json.loads(cache_row['classification']) if cache_row['classification'] else None
                    site_status = "CACHED"

        # Step 3-5: Crawl and classify
        if site_final and not classification:
            pages = crawl_site(site_final)
            corpus = '\n\n'.join([text for _, text in pages])
            content_hash = hashlib.md5(corpus.encode()).hexdigest()
            classification = classify_practice_areas(corpus, lawyer_name)
            site_status = site_status or ("CRAWLED" if pages else "NO_CONTENT")

            # Cache result
            conn.execute('''INSERT OR REPLACE INTO site_cache (url, site_final, classification, content_hash, last_checked)
                           VALUES (?,?,?,?,?)''',
                        (site_final, site_final, json.dumps(classification), content_hash, datetime.now().isoformat()))
            conn.commit()

        # Step 6: Business rules
        recommendation, reason = apply_business_rules(classification, config)

        # Step 7: Facebook
        facebook_found = facebook_input or ''
        if not facebook_found:
            fb = web_search_for_facebook(lawyer_name)
            if fb:
                facebook_found = fb

        # Extract fields
        primary = classification.get('primary_practice_areas', []) if classification else []
        secondary = classification.get('secondary_practice_areas', []) if classification else []
        evidence = classification.get('evidence', []) if classification else []
        confidence = classification.get('confidence', 0) if classification else 0

        conn.execute('''UPDATE lawyer_rows SET
            site_final=?, site_status=?,
            primary_area_1=?, primary_area_2=?, primary_area_3=?,
            secondary_areas=?, confidence=?,
            recommendation=?, recommendation_reason=?,
            evidence_1=?, evidence_2=?,
            facebook_found=?, checked_at=?, status='done'
            WHERE id=?''', (
            site_final, site_status,
            primary[0] if len(primary) > 0 else None,
            primary[1] if len(primary) > 1 else None,
            primary[2] if len(primary) > 2 else None,
            ', '.join(secondary),
            confidence,
            recommendation, reason,
            evidence[0] if len(evidence) > 0 else None,
            evidence[1] if len(evidence) > 1 else None,
            facebook_found, datetime.now().isoformat(),
            row_id
        ))
        conn.commit()

    except Exception as e:
        tb = traceback.format_exc()
        conn.execute("UPDATE lawyer_rows SET status='error', error=? WHERE id=?", (str(tb)[:500], row_id))
        conn.commit()
    finally:
        # Update job progress
        done = conn.execute("SELECT COUNT(*) as c FROM lawyer_rows WHERE job_id=? AND status IN ('done','error')", (job_id,)).fetchone()['c']
        conn.execute("UPDATE jobs SET processed_rows=? WHERE id=?", (done, job_id))
        conn.commit()
        conn.close()

def run_job(job_id):
    conn = get_db()
    conn.execute("UPDATE jobs SET status='running' WHERE id=?", (job_id,))
    conn.commit()

    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    config = json.loads(job['config']) if job['config'] else {}
    rows = conn.execute("SELECT * FROM lawyer_rows WHERE job_id=? ORDER BY row_index", (job_id,)).fetchall()
    conn.close()

    for row in rows:
        if row['status'] in ('done', 'error'):
            continue
        process_row(job_id, row['id'], row['raw_data'], config)
        time.sleep(0.5)  # Be polite

    conn = get_db()
    conn.execute("UPDATE jobs SET status='completed', completed_at=? WHERE id=?",
                (datetime.now().isoformat(), job_id))
    conn.commit()
    conn.close()

# ─────────────────────────────────────────
# API Routes
# ─────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('../frontend', 'index.html')

@app.route('/api/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400

    f = request.files['file']
    config_str = request.form.get('config', '{}')
    config = json.loads(config_str)

    filename = f.filename
    path = os.path.join(UPLOAD_DIR, f"{int(time.time())}_{filename}")
    f.save(path)

    # Parse Excel
    wb = openpyxl.load_workbook(path)
    ws = wb.active
    headers = [cell.value for cell in ws[1]]

    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO jobs (filename, status, total_rows, created_at, config) VALUES (?,?,?,?,?)",
              (filename, 'pending', ws.max_row - 1, datetime.now().isoformat(), json.dumps(config)))
    job_id = c.lastrowid

    for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True)):
        if not any(row):
            continue
        row_data = {headers[j]: row[j] for j in range(len(headers)) if j < len(row)}
        c.execute("INSERT INTO lawyer_rows (job_id, row_index, raw_data, status) VALUES (?,?,?,?)",
                  (job_id, i, json.dumps(row_data, ensure_ascii=False, default=str), 'pending'))

    total = c.execute("SELECT COUNT(*) FROM lawyer_rows WHERE job_id=?", (job_id,)).fetchone()[0]
    c.execute("UPDATE jobs SET total_rows=? WHERE id=?", (total, job_id))
    conn.commit()
    conn.close()

    # Start background job
    t = threading.Thread(target=run_job, args=(job_id,))
    t.daemon = True
    t.start()

    return jsonify({'job_id': job_id, 'total_rows': total})

@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    conn = get_db()
    jobs = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 20").fetchall()
    conn.close()
    return jsonify([dict(j) for j in jobs])

@app.route('/api/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id):
    conn = get_db()
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    conn.close()
    if not job:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(dict(job))

@app.route('/api/jobs/<int:job_id>/rows', methods=['GET'])
def get_rows(job_id):
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    filter_rec = request.args.get('recommendation', '')
    search = request.args.get('search', '')

    offset = (page - 1) * per_page
    conn = get_db()

    where = "WHERE job_id=?"
    params = [job_id]
    if filter_rec:
        where += " AND recommendation=?"
        params.append(filter_rec)
    if search:
        where += " AND (raw_data LIKE ? OR site_final LIKE ?)"
        params += [f'%{search}%', f'%{search}%']

    total = conn.execute(f"SELECT COUNT(*) FROM lawyer_rows {where}", params).fetchone()[0]
    rows = conn.execute(f"SELECT * FROM lawyer_rows {where} ORDER BY row_index LIMIT ? OFFSET ?",
                       params + [per_page, offset]).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = dict(r)
        d['raw_data'] = json.loads(d['raw_data']) if d['raw_data'] else {}
        result.append(d)

    return jsonify({'rows': result, 'total': total, 'page': page, 'per_page': per_page})

@app.route('/api/jobs/<int:job_id>/export', methods=['GET'])
def export_job(job_id):
    conn = get_db()
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    rows = conn.execute("SELECT * FROM lawyer_rows WHERE job_id=? ORDER BY row_index", (job_id,)).fetchall()
    conn.close()

    wb = Workbook()
    ws = wb.active
    ws.title = "Results"

    # Build headers from first row
    first_raw = json.loads(rows[0]['raw_data']) if rows else {}
    orig_headers = list(first_raw.keys())
    extra_headers = ['site_final', 'site_status', 'primary_area_1', 'primary_area_2', 'primary_area_3',
                     'secondary_areas', 'confidence', 'recommendation', 'recommendation_reason',
                     'evidence_1', 'evidence_2', 'facebook_found', 'checked_at']

    all_headers = orig_headers + extra_headers
    ws.append(all_headers)

    for row in rows:
        raw = json.loads(row['raw_data']) if row['raw_data'] else {}
        orig_vals = [raw.get(h, '') for h in orig_headers]
        extra_vals = [
            row['site_final'] or '', row['site_status'] or '',
            row['primary_area_1'] or '', row['primary_area_2'] or '', row['primary_area_3'] or '',
            row['secondary_areas'] or '', row['confidence'] or '',
            row['recommendation'] or '', row['recommendation_reason'] or '',
            row['evidence_1'] or '', row['evidence_2'] or '',
            row['facebook_found'] or '', row['checked_at'] or ''
        ]
        ws.append(orig_vals + extra_vals)

    path = os.path.join(EXPORT_DIR, f"enriched_{job_id}_{int(time.time())}.xlsx")
    wb.save(path)
    return send_file(path, as_attachment=True, download_name=f"enriched_lawyers_{job_id}.xlsx")

@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    conn = get_db()
    conn.execute("DELETE FROM site_cache")
    conn.commit()
    conn.close()
    return jsonify({'ok': True})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)

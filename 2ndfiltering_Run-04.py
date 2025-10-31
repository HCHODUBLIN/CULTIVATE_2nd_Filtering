import os
import re
import json
import time
import random
import hashlib
import pathlib
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse


# ===== BASE DIRECTORY =====
BASE_DIR = pathlib.Path(__file__).parent/ "Run-04" / "01--to-process"

OUTPUT_BASE = os.path.join(BASE_DIR, "_scraped_text")

REQUEST_TIMEOUT = 20
PAUSE_RANGE = (1.0, 2.0)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "Python-requests/BS4 (CULTIVATE research; contact: hyunjicho@tcd.ie)"
)

URL_CANDIDATE_COLS = [
    "URL", "Url", "url", "website", "Website", "link", "Link", "web", "homepage"
]

def ensure_dir(path: str) -> None:
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def safe_file_stem(url: str) -> str:
    """Make a safe filename from the URL."""
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    host = urlparse(url).netloc.replace(":", "_")
    return f"{host}__{h}"

def extract_visible_text(html: str) -> str:
    """Remove scripts/styles and extract visible text."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "template", "svg", "canvas"]):
        tag.decompose()
    for tagname in ["nav", "footer", "aside"]:
        for t in soup.find_all(tagname):
            t.decompose()
    text = soup.get_text(separator="\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text)

def fetch(url: str):
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        return r.status_code, r.url, r.text, None
    except requests.RequestException as e:
        return None, None, None, str(e)

# ---------- New: generic loader for xlsx/csv/json/ndjson ----------
def load_table_any(path: str) -> pd.DataFrame:
    ext = pathlib.Path(path).suffix.lower()
    # Excel
    if ext in [".xlsx", ".xls"]:
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception:
            return pd.read_excel(path)
    # CSV
    if ext == ".csv":
        return pd.read_csv(path)
    # JSON (array, object with list, or NDJSON)
    if ext in [".json", ".ndjson", ".jsonl"]:
        # Try NDJSON first
        try:
            return pd.read_json(path, lines=True)
        except Exception:
            pass
        # Try regular JSON
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            # Common patterns: {"data":[...]}, {"rows":[...]}, {"items":[...]}
            for key in ["data", "rows", "items", "results"]:
                if key in data and isinstance(data[key], list):
                    return pd.DataFrame(data[key])
            # Fallback: single-record dict
            return pd.DataFrame([data])
    raise ValueError(f"Unsupported file type or unreadable file: {path}")

def detect_url_column(df: pd.DataFrame) -> str | None:
    cols = [str(c).strip() for c in df.columns]
    # direct matches (case-insensitive)
    for c in cols:
        if c in URL_CANDIDATE_COLS or c.lower() in [x.lower() for x in URL_CANDIDATE_COLS]:
            return c
    # heuristic: any column containing 'url' or 'website'
    for c in cols:
        cl = c.lower()
        if "url" in cl or "site" in cl or "web" in cl or "link" in cl:
            return c
    return None

def process_table(input_path: str):
    stem = pathlib.Path(input_path).stem
    # Derive city name: if your naming is City_results.* keep it; else use filename
    city_name = stem.replace("_results", "")
    out_dir = os.path.join(OUTPUT_BASE, city_name)
    ensure_dir(out_dir)
    summary_csv = os.path.join(out_dir, "scrape_summary.csv")

    df = load_table_any(input_path)
    df.columns = [str(c).strip() for c in df.columns]

    url_col = detect_url_column(df)
    if not url_col:
        print(f"[WARN] No URL-like column found in: {input_path}")
        return

    # Collect URLs
    urls = []
    for idx, val in df[url_col].items():
        if isinstance(val, str):
            v = val.strip()
            if v.startswith(("http://", "https://")):
                urls.append((idx, v))
    if not urls:
        print(f"[{city_name}] No URLs found in {input_path}.")
        return

    print(f"[{city_name}] {len(urls)} URL(s) in {os.path.basename(input_path)}.")

    summary = []
    for i, (row_idx, url) in enumerate(urls, 1):
        print(f"  [{i}/{len(urls)}] {url}")
        status, final_url, html, err = fetch(url)

        txt_stem = safe_file_stem(url)
        txt_path = os.path.join(out_dir, f"{txt_stem}.txt")

        title, text = None, None
        if err is None and status and html:
            soup = BeautifulSoup(html, "html.parser")
            if soup.title and soup.title.string:
                title = soup.title.string.strip()
            text = extract_visible_text(html)
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(text)

        summary.append(
            {
                "row": int(row_idx),
                "url": url,
                "final_url": final_url,
                "status": status,
                "error": err,
                "title": title,
                "text_file": txt_path if text else None,
            }
        )
        time.sleep(random.uniform(*PAUSE_RANGE))

    pd.DataFrame(summary).to_csv(summary_csv, index=False)
    print(f"  ✓ Saved: {summary_csv}")

def main():
    # find all candidate files in the folder
    exts = (".xlsx", ".xls", ".csv", ".json", ".ndjson", ".jsonl")
    files = sorted([str(p) for p in pathlib.Path(BASE_DIR).glob("*") if p.suffix.lower() in exts])
    if not files:
        print(f"No input files with {exts} found in {BASE_DIR}")
        return

    print(f"Found {len(files)} file(s) in {BASE_DIR}\n")
    for fp in files:
        print(f"Processing: {pathlib.Path(fp).name}")
        try:
            process_table(fp)
        except Exception as e:
            print(f"[ERROR] {fp}: {e}")

if __name__ == "__main__":
    main()

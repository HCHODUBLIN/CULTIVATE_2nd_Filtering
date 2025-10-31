import os
import re
import time
import random
import hashlib
import pathlib
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# ====== CONFIG ======
BASE_DIR = pathlib.Path(__file__).parent/ "Run-03" / "01--to-process"

OUTPUT_BASE = os.path.join(BASE_DIR, "_scraped_text")

REQUEST_TIMEOUT = 20
PAUSE_RANGE = (1.0, 2.0)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "Python-requests/BS4 (CULTIVATE research; contact: hyunjicho@tcd.ie)"
)

def ensure_dir(path: str) -> None:
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def safe_file_stem(url: str) -> str:
    """Make a safe filename from the URL"""
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    host = urlparse(url).netloc.replace(":", "_")
    return f"{host}__{h}"

def extract_visible_text(html: str) -> str:
    """Remove scripts/styles and extract visible text"""
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

def process_excel(excel_path: str):
    city_name = pathlib.Path(excel_path).stem.replace("_results", "")
    out_dir = os.path.join(OUTPUT_BASE, city_name)
    ensure_dir(out_dir)
    summary_csv = os.path.join(out_dir, "scrape_summary.csv")

    try:
        df = pd.read_excel(excel_path, engine="openpyxl")
    except Exception:
        df = pd.read_excel(excel_path)

    df.columns = [str(c).strip() for c in df.columns]

    # find URL column (case-insensitive)
    url_col = None
    if "URL" in df.columns:
        url_col = "URL"
    else:
        for c in df.columns:
            if c.strip().lower() == "url":
                url_col = c
                break
    if not url_col:
        print(f"[WARN] No URL column in: {excel_path}")
        return

    # collect URLs
    urls = []
    for idx, val in df[url_col].items():
        if isinstance(val, str) and val.strip().startswith(("http://", "https://")):
            urls.append((idx, val.strip()))

    if not urls:
        print(f"[{city_name}] No URLs found.")
        return

    print(f"[{city_name}] {len(urls)} URL(s).")

    summary = []
    for i, (row_idx, url) in enumerate(urls, 1):
        print(f"  [{i}/{len(urls)}] {url}")
        status, final_url, html, err = fetch(url)

        stem = safe_file_stem(url)
        txt_path = os.path.join(out_dir, f"{stem}.txt")

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
    # find all .xlsx files in the folder
    excel_files = sorted([str(p) for p in pathlib.Path(BASE_DIR).glob("*.xlsx")])
    if not excel_files:
        print(f"No .xlsx files found in {BASE_DIR}")
        return

    print(f"Found {len(excel_files)} Excel file(s) in {BASE_DIR}\n")
    for xlf in excel_files:
        print(f"Processing: {pathlib.Path(xlf).name}")
        process_excel(xlf)

if __name__ == "__main__":
    main()

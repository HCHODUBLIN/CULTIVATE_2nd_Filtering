import os, re, time, random, hashlib, pathlib, argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# ---------- helpers ----------
def ensure_dir(path: str) -> None:
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def safe_file_stem(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    host = urlparse(url).netloc.replace(":", "_")
    return f"{host}__{h}"

def extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "template", "svg", "canvas"]):
        tag.decompose()
    for tagname in ["nav", "footer", "aside"]:
        for t in soup.find_all(tagname):
            t.decompose()
    text = soup.get_text(separator="\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text)

def fetch(url: str, ua: str, timeout: int):
    try:
        r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout)
        return r.status_code, r.url, r.text, None
    except requests.RequestException as e:
        return None, None, None, str(e)

# ---------- core ----------
def process_excel(excel_path: str, output_base: str, ua: str, timeout: int, pause_range: tuple[float, float]):
    city_name = pathlib.Path(excel_path).stem.replace("_results", "")
    out_dir = os.path.join(output_base, city_name)
    ensure_dir(out_dir)
    summary_csv = os.path.join(out_dir, "scrape_summary.csv")

    # robust Excel load (keeps your previous behaviour)
    try:
        df = pd.read_excel(excel_path, engine="openpyxl")
    except Exception:
        df = pd.read_excel(excel_path)

    df.columns = [str(c).strip() for c in df.columns]

    # detect URL column
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
        status, final_url, html, err = fetch(url, ua=ua, timeout=timeout)

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
        time.sleep(random.uniform(*pause_range))

    pd.DataFrame(summary).to_csv(summary_csv, index=False)
    print(f"  ✓ Saved: {summary_csv}")

def main():
    p = argparse.ArgumentParser(description="Scrape visible text for URLs listed in Excel files.")
    # paths
    p.add_argument("--base-dir", type=pathlib.Path,
                   default=pathlib.Path(__file__).parent / "Run-03" / "03--archived",
                   help="Directory containing the Excel files to process")
    p.add_argument("--output-base", type=pathlib.Path,
                   default=None,
                   help="Output directory for scraped text; defaults to <base-dir>/_scraped_text")
    # networking
    p.add_argument("--timeout", type=int, default=20, help="Request timeout (seconds)")
    p.add_argument("--pause-min", type=float, default=1.0, help="Min pause between requests (s)")
    p.add_argument("--pause-max", type=float, default=2.0, help="Max pause between requests (s)")
    p.add_argument("--user-agent", type=str,
                   default=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "Python-requests/BS4 (CULTIVATE research; contact: hyunjicho@tcd.ie)"),
                   help="HTTP User-Agent string")

    args = p.parse_args()

    base_dir: pathlib.Path = args.base_dir
    output_base = args.output_base or (base_dir / "_scraped_text")
    pause_range = (args.pause_min, args.pause_max)

    excel_files = sorted([str(p) for p in base_dir.glob("*.xlsx")])
    if not excel_files:
        print(f"No .xlsx files found in {base_dir}")
        return

    print(f"Found {len(excel_files)} Excel file(s) in {base_dir}\n")
    for xlf in excel_files:
        print(f"Processing: {pathlib.Path(xlf).name}")
        process_excel(xlf, str(output_base), args.user_agent, args.timeout, pause_range)

if __name__ == "__main__":
    main()

import os
import pathlib
import pandas as pd

# ===== BASE DIRECTORY =====
BASE_DIR = pathlib.Path(__file__).parent

# ==== PATHS (edit if needed) ====


RUN_DIR = BASE_DIR / "Run-03-2ndFilteringImproved" 
SOURCE_DIR = BASE_DIR / "Run-03-2ndFiltering"                     # where the original *_results.xlsx live
SCRAPED_BASE = SOURCE_DIR / "_scraped_text"                 # where _scraped_text/<City>/scrape_summary.csv live
FILTER_CSV = RUN_DIR / "fsi_filter_results_improved.csv"              # output from analyse_fsi_filter.py
OUTPUT_XLSX = RUN_DIR / "FSI_included_combined_improved.xlsx"        # final combined Excel
OUTPUT_CSV = RUN_DIR / "FSI_included_combined_improved.csv"         # optional CSV

# Target column order (create blanks for any missing columns)
TARGET_COLS = [
    "City", "Country", "Name", "URL", "Facebook URL", "Twitter URL", "Instagram URL",
    "Food Sharing Activities", "How it is Shared", "Date Checked", "Comments", "Lat", "Lon"
]

def load_included_url_ids(filter_csv: str) -> set:
    """Read the LLM filter results and keep only include decisions as a set of url_ids (host__hash)."""
    df = pd.read_csv(filter_csv)
    df.columns = [str(c).strip() for c in df.columns]
    if "decision" not in df.columns or "url_id" not in df.columns:
        raise ValueError("Expected columns 'decision' and 'url_id' in fsi_filter_results.csv")
    inc = df[df["decision"].astype(str).str.lower() == "include"]["url_id"].dropna().astype(str).str.strip()
    return set(inc)

def find_scrape_summaries(scraped_base: str) -> list:
    """Find every per-city scrape_summary.csv under _scraped_text/<City>/."""
    paths = []
    base = pathlib.Path(scraped_base)
    if not base.exists():
        return paths
    for p in base.glob("*/scrape_summary.csv"):
        paths.append(str(p))
    return paths

def collect_included_rows(included_ids: set, summaries: list) -> pd.DataFrame:
    """
    From all scrape_summary.csv files, keep rows whose text_file basename (without .txt)
    matches an included url_id. Return a DataFrame with source excel & row indices.
    """
    hits = []
    for summ_path in summaries:
        city = pathlib.Path(summ_path).parent.name
        df = pd.read_csv(summ_path)
        # robust column names
        df.columns = [str(c).strip() for c in df.columns]
        if "text_file" not in df.columns or "row" not in df.columns or "url" not in df.columns:
            # older/modified schemas: try to proceed best-effort
            print(f"[WARN] Unexpected columns in {summ_path}; expected at least text_file, row, url.")
        # Derive url_id from text_file (basename without extension)
        if "text_file" in df.columns:
            url_ids = df["text_file"].fillna("").astype(str).apply(
                lambda s: pathlib.Path(s).stem if s else ""
            )
        else:
            # If text_file missing, we cannot map; skip this summary
            print(f"[WARN] No 'text_file' column in {summ_path}; skipping.")
            continue

        mask = url_ids.isin(included_ids)
        kept = df[mask].copy()
        if kept.empty:
            continue

        kept["city_from_summary"] = city
        kept["url_id"] = url_ids[mask].values
        hits.append(kept)

    if not hits:
        return pd.DataFrame(columns=["source_excel", "row", "url", "url_id", "city_from_summary"])

    out = pd.concat(hits, ignore_index=True)
    # Try to carry the original source Excel file if present in summary
    # If not present, infer from 'sources' or from city name + pattern
    if "source_files" in out.columns:
        out["source_excel"] = out["source_files"].str.split(";").str[0].str.strip()
    else:
        # Fallback: infer source Excel path as BASE/<City>_results.xlsx
        out["source_excel"] = out["city_from_summary"].apply(lambda c: os.path.join(SOURCE_DIR, f"{c}_results.xlsx"))

    # Ensure row is integer index to select from original Excel
    if "row" in out.columns:
        out["row"] = pd.to_numeric(out["row"], errors="coerce").astype("Int64")
    return out

def load_original_rows(included_map: pd.DataFrame) -> pd.DataFrame:
    """
    Read the original Excel files and pick the exact rows by index from 'row' column.
    Return concatenated data with a 'Source File' column.
    """
    if included_map.empty:
        return pd.DataFrame(columns=TARGET_COLS + ["Source File"])

    collected = []
    # Group by source Excel to minimise file I/O
    for src, group in included_map.groupby("source_excel"):
        src_path = str(src)
        if not os.path.exists(src_path):
            # Try filename only (it may be name rather than full path)
            cand = os.path.join(SOURCE_DIR, pathlib.Path(src_path).name)
            if os.path.exists(cand):
                src_path = cand
            else:
                print(f"[WARN] Source Excel not found: {src}")
                continue

        try:
            df = pd.read_excel(src_path, engine="openpyxl")
        except Exception:
            df = pd.read_excel(src_path)

        df.columns = [str(c).strip() for c in df.columns]
        # Rows to take (pandas default index from original reading)
        rows = group["row"].dropna().astype(int).unique().tolist()
        sub = df.iloc[rows].copy()

        # Add a source file marker (filename only)
        sub["Source File"] = pathlib.Path(src_path).name

        # Ensure all TARGET_COLS exist (create blanks if missing)
        for col in TARGET_COLS:
            if col not in sub.columns:
                sub[col] = pd.NA

        # Reorder columns (TARGET_COLS first, then anything else, plus Source File at end)
        other_cols = [c for c in sub.columns if c not in TARGET_COLS + ["Source File"]]
        sub = sub[TARGET_COLS + other_cols + ["Source File"]]

        collected.append(sub)

    if not collected:
        return pd.DataFrame(columns=TARGET_COLS + ["Source File"])
    return pd.concat(collected, ignore_index=True)

def main():
    if not os.path.exists(FILTER_CSV):
        raise FileNotFoundError(f"Filter CSV not found: {FILTER_CSV}")

    included_ids = load_included_url_ids(FILTER_CSV)
    if not included_ids:
        print("No 'include' decisions found in the filter CSV.")
        return

    summaries = find_scrape_summaries(SCRAPED_BASE)
    if not summaries:
        print(f"No scrape_summary.csv files found under: {SCRAPED_BASE}")
        return

    included_map = collect_included_rows(included_ids, summaries)
    if included_map.empty:
        print("No included rows mapped from summaries. Nothing to export.")
        return

    final_df = load_original_rows(included_map)
    if final_df.empty:
        print("No rows loaded from original Excels. Nothing to export.")
        return

    # Save to Excel (and CSV)
    final_df.to_excel(OUTPUT_XLSX, index=False)
    final_df.to_csv(OUTPUT_CSV, index=False)

    print(f"\nSaved combined included rows to:\n- {OUTPUT_XLSX}\n- {OUTPUT_CSV}")
    print(f"Total included rows: {len(final_df)}")

if __name__ == "__main__":
    main()

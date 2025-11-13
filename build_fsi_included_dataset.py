import os
import argparse
import pathlib
import pandas as pd
from typing import Set, List


# Target column order (create blanks for any missing columns)
TARGET_COLS = [
    "City", "Country", "Name", "URL", "Facebook URL", "Twitter URL", "Instagram URL",
    "Food Sharing Activities", "How it is Shared", "Date Checked", "Comments", "Lat", "Lon"
]


def load_included_url_ids(filter_csv: pathlib.Path) -> Set[str]:
    """
    Read the LLM filter results and keep only 'include' decisions as a set of url_ids (host__hash).
    """
    df = pd.read_csv(filter_csv)
    df.columns = [str(c).strip() for c in df.columns]

    if "decision" not in df.columns or "url_id" not in df.columns:
        raise ValueError("Expected columns 'decision' and 'url_id' in fsi_filter_results.csv")

    inc = (
        df[df["decision"].astype(str).str.lower() == "include"]["url_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    return set(inc)


def find_scrape_summaries(scraped_base: pathlib.Path) -> List[pathlib.Path]:
    """
    Find every per-city scrape_summary.csv under _scraped_text/<City>/.
    """
    paths: List[pathlib.Path] = []
    if not scraped_base.exists():
        return paths

    for p in scraped_base.glob("*/scrape_summary.csv"):
        paths.append(p)

    return paths


def collect_included_rows(
    included_ids: Set[str],
    summaries: List[pathlib.Path],
    source_dir: pathlib.Path,
) -> pd.DataFrame:
    """
    From all scrape_summary.csv files, keep rows whose text_file basename (without .txt)
    matches an included url_id. Return a DataFrame with source excel & row indices.
    """
    hits = []

    for summ_path in summaries:
        city = summ_path.parent.name
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
    # If not present, infer from 'source_files' or from city name + pattern
    if "source_files" in out.columns:
        out["source_excel"] = out["source_files"].str.split(";").str[0].str.strip()
    else:
        # Fallback: infer source Excel path as <source_dir>/<City>_results.xlsx
        out["source_excel"] = out["city_from_summary"].apply(
            lambda c: str(source_dir / f"{c}_results.xlsx")
        )

    # Ensure row is integer index to select from original Excel
    if "row" in out.columns:
        out["row"] = pd.to_numeric(out["row"], errors="coerce").astype("Int64")

    return out


def load_original_rows(
    included_map: pd.DataFrame,
    source_dir: pathlib.Path,
) -> pd.DataFrame:
    """
    Read the original Excel files and pick the exact rows by index from 'row' column.
    Return concatenated data with a 'Source File' column.
    """
    if included_map.empty:
        return pd.DataFrame(columns=TARGET_COLS + ["Source File"])

    collected = []

    # Group by source Excel to minimise file I/O
    for src, group in included_map.groupby("source_excel"):
        src_path = pathlib.Path(str(src))

        if not src_path.exists():
            # Try filename only (it may be name rather than full path)
            cand = source_dir / src_path.name
            if cand.exists():
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
        sub["Source File"] = src_path.name

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


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    base_dir = pathlib.Path(__file__).parent
    default_run_dir = base_dir / "Run-03"
    default_source_dir = default_run_dir / "01--to-process"
    default_scraped_base = default_source_dir / "_scraped_text"
    default_filter_csv = default_run_dir / "fsi_filter_results.csv"
    default_output_xlsx = default_run_dir / "FSI_included_combined.xlsx"
    default_output_csv = default_run_dir / "FSI_included_combined.csv"

    parser = argparse.ArgumentParser(
        description="Combine included FSIs from filter results, scrape summaries, and original Excel files."
    )

    parser.add_argument(
        "--run-dir",
        type=pathlib.Path,
        default=default_run_dir,
        help=f"Base run directory (default: {default_run_dir})",
    )
    parser.add_argument(
        "--source-dir",
        type=pathlib.Path,
        default=default_source_dir,
        help=f"Directory containing original *_results.xlsx files (default: {default_source_dir})",
    )
    parser.add_argument(
        "--scraped-base",
        type=pathlib.Path,
        default=default_scraped_base,
        help=f"Base directory where _scraped_text/<City>/scrape_summary.csv live (default: {default_scraped_base})",
    )
    parser.add_argument(
        "--filter-csv",
        type=pathlib.Path,
        default=default_filter_csv,
        help=f"Filter CSV produced by analyse_fsi_filter.py (default: {default_filter_csv})",
    )
    parser.add_argument(
        "--output-xlsx",
        type=pathlib.Path,
        default=default_output_xlsx,
        help=f"Path for combined output Excel (default: {default_output_xlsx})",
    )
    parser.add_argument(
        "--output-csv",
        type=pathlib.Path,
        default=default_output_csv,
        help=f"Path for combined output CSV (default: {default_output_csv})",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="If set, do not write the CSV version (only Excel).",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_dir = args.run_dir
    source_dir = args.source_dir
    scraped_base = args.scraped_base
    filter_csv = args.filter_csv
    output_xlsx = args.output_xlsx
    output_csv = args.output_csv

    if not filter_csv.exists():
        raise FileNotFoundError(f"Filter CSV not found: {filter_csv}")

    included_ids = load_included_url_ids(filter_csv)
    if not included_ids:
        print("No 'include' decisions found in the filter CSV.")
        return

    summaries = find_scrape_summaries(scraped_base)
    if not summaries:
        print(f"No scrape_summary.csv files found under: {scraped_base}")
        return

    included_map = collect_included_rows(included_ids, summaries, source_dir=source_dir)
    if included_map.empty:
        print("No included rows mapped from summaries. Nothing to export.")
        return

    final_df = load_original_rows(included_map, source_dir=source_dir)
    if final_df.empty:
        print("No rows loaded from original Excels. Nothing to export.")
        return

    # Save to Excel (and CSV)
    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_excel(output_xlsx, index=False)

    if not args.no_csv:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(output_csv, index=False)

    print("\nSaved combined included rows to:")
    print(f"- {output_xlsx}")
    if not args.no_csv:
        print(f"- {output_csv}")
    print(f"Total included rows: {len(final_df)}")


if __name__ == "__main__":
    main()

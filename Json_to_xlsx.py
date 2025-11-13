import os
import json
import argparse
import pathlib
import pandas as pd
from pandas import json_normalize

# URL candidate column names (case-insensitive)
URL_CANDIDATES = [
    "url", "website", "link", "homepage", "web", "site", "page_url"
]


def find_inputs(base: pathlib.Path):
    """Find JSON/JSONL/NDJSON files in the directory."""
    exts = (".json", ".jsonl", ".ndjson")
    return sorted(
        [p for p in base.glob("*") if p.suffix.lower() in exts]
    )


def load_json_any(path: pathlib.Path) -> pd.DataFrame:
    """Handle JSON or NDJSON/JSONL automatically."""
    try:
        return pd.read_json(path, lines=True)
    except Exception:
        pass

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, list):
        return pd.DataFrame(raw)

    if isinstance(raw, dict):
        for key in ("data", "items", "rows", "results", "records"):
            if key in raw and isinstance(raw[key], list):
                return pd.DataFrame(raw[key])
        return json_normalize(raw, sep=".")

    return pd.DataFrame([{"_value": raw}])


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove whitespace/newlines from column names."""
    df.columns = [
        str(c).strip().replace("\n", " ").replace("\r", " ")
        for c in df.columns
    ]
    return df


def promote_url_column(df: pd.DataFrame) -> pd.DataFrame:
    """Automatically rename one column to 'URL' if applicable."""
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}

    if "URL" in cols:
        return df

    for cand in URL_CANDIDATES:
        if cand in lower_map:
            src = lower_map[cand]
            return df.rename(columns={src: "URL"})

    for c in cols:
        if "url" in c.lower():
            return df.rename(columns={c: "URL"})

    return df


def to_excel_safe(df: pd.DataFrame, out_path: pathlib.Path):
    """Save DataFrame safely as Excel."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="Sheet1")


def process_file(path: pathlib.Path, output_suffix: str):
    """Process a single JSON file and export Excel."""
    df = load_json_any(path)
    df = normalise_columns(df)
    df = promote_url_column(df)

    stem = path.stem
    if stem.endswith("_results"):
        out_name = f"{stem}.xlsx"
    else:
        out_name = f"{stem}{output_suffix}"

    out_path = path.with_name(out_name)
    to_excel_safe(df, out_path)

    print(f"✓ {path.name} → {out_path.name}  (rows: {len(df)})")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    default_base_dir = pathlib.Path(__file__).parent / "Run-04" / "01--to-process"
    default_suffix = "_results.xlsx"

    parser = argparse.ArgumentParser(
        description="Convert JSON/NDJSON files to Excel result files."
    )

    parser.add_argument(
        "--base-dir",
        type=pathlib.Path,
        default=default_base_dir,
        help=f"Input directory containing JSON/NDJSON files (default: {default_base_dir})",
    )
    parser.add_argument(
        "--output-suffix",
        type=str,
        default=default_suffix,
        help=f"Suffix for output Excel filenames (default: '{default_suffix}')",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    base_dir = args.base_dir
    output_suffix = args.output_suffix

    inputs = find_inputs(base_dir)
    if not inputs:
        print(f"No JSON/NDJSON files found in {base_dir}")
        return

    print(f"Found {len(inputs)} file(s) in {base_dir}")

    for p in inputs:
        try:
            process_file(p, output_suffix=output_suffix)
        except Exception as e:
            print(f"[ERROR] {p.name}: {e}")


if __name__ == "__main__":
    main()

import pathlib
import pandas as pd

# ===== BASE DIRECTORY =====
BASE_DIR = pathlib.Path(__file__).parent

# ===== FOLDERS =====
DIR_FILTERED = BASE_DIR / "Run-04-2ndFiltering"  # 2nd filtering results
DIR_ORIGINAL = BASE_DIR / "Run-04" / "01--to-process"              # Before filtering
DIR_VERIFIED = BASE_DIR / "Run-04" / "02--completed"             # Final verified

def count_rows_in_folder(folder: pathlib.Path) -> pd.DataFrame:
    """
    Count the number of rows in all Excel files within a given folder.
    Returns a DataFrame with columns: folder, file, rows.
    """
    records = []
    for xlsx_file in sorted(folder.glob("*.xlsx")):
        try:
            df = pd.read_excel(xlsx_file)
            records.append({
                "folder": folder.name,
                "file": xlsx_file.name,
                "rows": len(df)
            })
        except Exception as e:
            print(f"⚠️ Error reading {xlsx_file.name}: {e}")
    return pd.DataFrame(records)

def main():
    # Count rows in each folder
    df_filtered = count_rows_in_folder(DIR_FILTERED)
    df_original = count_rows_in_folder(DIR_ORIGINAL)
    df_verified = count_rows_in_folder(DIR_VERIFIED)

    # Combine all results
    all_counts = pd.concat([df_filtered, df_original, df_verified],
                           ignore_index=True)

    # Summarise by folder (total number of rows)
    summary = (
        all_counts
        .groupby("folder", as_index=False)["rows"]
        .sum()
        .rename(columns={"rows": "total_rows"})
    )

    # Save results
    OUTPUT_DETAIL = BASE_DIR / "Run-03" / "entry_counts_detailed.csv"
    OUTPUT_SUMMARY = BASE_DIR / "Run-03" / "entry_counts_summary.csv"

    all_counts.to_csv(OUTPUT_DETAIL, index=False)
    summary.to_csv(OUTPUT_SUMMARY, index=False)

    # Print results
    print("=== Detailed (per file) ===")
    print(all_counts)
    print()
    print("=== Summary (per folder) ===")
    print(summary)

if __name__ == "__main__":
    main()

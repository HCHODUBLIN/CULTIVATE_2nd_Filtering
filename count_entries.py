import argparse
import pathlib
import pandas as pd


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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Count Excel row entries across multiple folders."
    )

    parser.add_argument(
        "--filtered-dir",
        type=pathlib.Path,
        required=True,
        help="Folder containing 2nd filtered Excel files",
    )

    parser.add_argument(
        "--original-dir",
        type=pathlib.Path,
        required=True,
        help="Folder containing original archived Excel files",
    )

    parser.add_argument(
        "--verified-dir",
        type=pathlib.Path,
        required=True,
        help="Folder containing final verified Excel files",
    )

    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        required=True,
        help="Directory to save the CSV outputs",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Folders (all required, no defaults)
    dir_filtered = args.filtered_dir
    dir_original = args.original_dir
    dir_verified = args.verified_dir

    # Output directory (required)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Count rows
    df_filtered = count_rows_in_folder(dir_filtered)
    df_original = count_rows_in_folder(dir_original)
    df_verified = count_rows_in_folder(dir_verified)

    # Combine
    all_counts = pd.concat(
        [df_filtered, df_original, df_verified],
        ignore_index=True
    )

    summary = (
        all_counts
        .groupby("folder", as_index=False)["rows"]
        .sum()
        .rename(columns={"rows": "total_rows"})
    )

    # Save outputs
    detail_path = output_dir / "entry_counts_detailed.csv"
    summary_path = output_dir / "entry_counts_summary.csv"

    all_counts.to_csv(detail_path, index=False)
    summary.to_csv(summary_path, index=False)

    # Print
    print("=== Detailed (per file) ===")
    print(all_counts)
    print()
    print("=== Summary (per folder) ===")
    print(summary)
    print("\nSaved:")
    print(f"- {detail_path}")
    print(f"- {summary_path}")


if __name__ == "__main__":
    main()

import argparse
import pathlib
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge all Excel files in a folder into one Excel file."
    )

    parser.add_argument(
        "--input-dir",
        type=pathlib.Path,
        required=True,
        help="Folder containing Excel files to merge."
    )

    parser.add_argument(
        "--output-file",
        type=pathlib.Path,
        required=True,
        help="Path to the output merged Excel file."
    )

    return parser.parse_args()


def main():
    args = parse_args()

    input_dir = args.input_dir
    output_file = args.output_file

    excel_files = sorted(input_dir.glob("*.xlsx"))

    if not excel_files:
        print(f"No Excel files found in: {input_dir}")
        return

    print(f"Found {len(excel_files)} Excel files.")
    frames = []

    for x in excel_files:
        try:
            df = pd.read_excel(x)
            df["SourceFile"] = x.name   # optional: track origin
            frames.append(df)
            print(f"✓ Loaded {x.name} ({len(df)} rows)")
        except Exception as e:
            print(f"⚠️ Error reading {x.name}: {e}")

    if not frames:
        print("No valid Excel files could be read.")
        return

    merged = pd.concat(frames, ignore_index=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    merged.to_excel(output_file, index=False)

    print("\n=== Merge Completed ===")
    print(f"Total rows: {len(merged)}")
    print(f"Saved merged file to:\n{output_file}")


if __name__ == "__main__":
    main()

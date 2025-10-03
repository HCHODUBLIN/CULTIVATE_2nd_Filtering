import pathlib
import pandas as pd

# ===== CONFIG =====
BASE_DIR = "/Users/hyunjicho/Documents/CULTIVATE_2nd_Filtering/Run-03/01--to-process"
OUTPUT_FILE = "/Users/hyunjicho/Documents/CULTIVATE_2nd_Filtering/Run-03/entry_counts.csv"

def main():
    base = pathlib.Path(BASE_DIR)
    excel_files = sorted(base.glob("*.xlsx"))

    if not excel_files:
        print(f"No .xlsx files found in {BASE_DIR}")
        return

    results = []

    total_rows = 0
    for f in excel_files:
        try:
            df = pd.read_excel(f, engine="openpyxl")
        except Exception:
            df = pd.read_excel(f)

        row_count = len(df)
        total_rows += row_count
        results.append({"file": f.name, "entries": row_count})

        print(f"{f.name}: {row_count} entries")

    results.append({"file": "TOTAL", "entries": total_rows})

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_FILE, index=False)

    print("\nSaved summary to:", OUTPUT_FILE)

if __name__ == "__main__":
    main()

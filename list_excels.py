import pathlib

# Correct folder containing your Excel files
BASE_DIR = pathlib.Path(__file__).parent / "Run-03" / "01--to-process"

def main():
    folder = pathlib.Path(BASE_DIR)
    
    # List all .xlsx files (non-recursive)
    excel_files = list(folder.glob("*.xlsx"))

    if not excel_files:
        print(f"No .xlsx files found in {BASE_DIR}")
        return

    print(f"Found {len(excel_files)} Excel files:\n")
    for f in excel_files:
        print(f.name)       # file name only
        # print(f)          # full path, if you prefer

if __name__ == "__main__":
    main()

# CULTIVATE_2nd_Filtering

Pipeline:
1) `2ndfiltering.py` — scrape URLs from Excel files and save text.
2) `analyse_fsi_filter.py` — classify FSI include/exclude via OpenAI API.
3) `build_fsi_included_dataset.py` — rebuild combined Excel of included FSIs.

## Setup
- Python: use `.venv/`
- Install: `pip install -r requirements.txt`
- Env: `export OPENAI_API_KEY="sk-..."`

## Data
- Inputs: `Run-03/01--to-process/*.xlsx`
- Outputs: `Run-03/01--to-process/_scraped_text/`, `Run-03/*.csv|.xlsx`

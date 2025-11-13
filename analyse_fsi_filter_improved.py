import os
import csv
import time
import json
import pathlib
from typing import List, Dict, Any

from openai import OpenAI, RateLimitError, APIError
from dotenv import load_dotenv
import argparse

# ========= ENV & OpenAI client =========
load_dotenv()  

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError(
        "❌ OPENAI_API_KEY not found. Please create a .env file containing:\n"
        "OPENAI_API_KEY=sk-your-key-here"
    )

client = OpenAI(api_key=api_key)

# ========= CONFIG  =========
MODEL = "gpt-4o-mini"      
MAX_CHARS_DEFAULT = 12000  
MAX_RETRIES = 5
BACKOFF_BASE = 2.0
PAUSE_DEFAULT = 0.2        

# ========= PROMPT =========
INSTRUCTIONS = """
You are a careful research assistant. Classify each website TEXT as an FSI (Food Sharing Initiative) INCLUDE or EXCLUDE.

Use these rules:

✅ Inclusion criteria (FSI websites to keep)
A website should be classified as an FSI if:
1) The website itself belongs to or directly represents the initiative/organisation (e.g., a food bank, community fridge, solidarity kitchen, cooperative garden, local library running a seed/food-sharing programme, etc.).
2) The initiative has a clear activity related to food sharing (redistribution of surplus food, free meals, seed/plant exchanges, shared kitchens, communal gardens, food clubs, etc.).
3) The website shows that the initiative is organised and ongoing (not just a one-off event).
4) The initiative can be formally recognised or community-based, as long as its main purpose is food sharing or access to food.

❌ Exclusion criteria (websites to exclude)
A website should not be classified as an FSI if:
1) It is only a personal blog, news article, magazine, or government site reporting about an initiative, but not the initiative’s own site.
2) It belongs to a media outlet, advocacy group, or municipality that only introduces/promotes FSIs, rather than running them.
3) It describes food-related activities but the website’s main purpose is unrelated to food sharing (e.g., political movement, general community activism, or a commercial site).
4) The initiative is mentioned only indirectly through an external article or story, with no direct ownership or representation on the site.
5) Institutional, educational, or cultural projects (such as museums, schools, or research centres) that only *host*, *exhibit*, or *collaborate on* food-related events, without being dedicated food-sharing initiatives.
6) Crowdfunding or fundraising platforms (e.g. YouBeHero, Crowdfunder, Produzioni dal Basso) where FSIs are *listed* as causes but the websites themselves do not *run* any food-sharing activity.
7) Media or publication sites (magazines, newspapers, blogs) that *publish stories about FSIs* but are *not operated by* them.
8) Municipality or official pages that mention FSIs as part of a civic programme but are not run by the initiative.
9) Short pages or external listings with little content or no organisational information should be excluded.

Return STRICT JSON only (no extra text), with this schema:
{
  "decision": "include" | "exclude",
  "confidence": 1..5,
  "reasons": [string, ...],
  "evidence_quotes": [string, ...],
  "organisation_name": string | null,
  "organisation_type": "food_bank" | "community_fridge" | "solidarity_kitchen" | "communal_garden" | "seed_library" | "food_club" | "social_supermarket" | "charity" | "cooperative" | "other" | null,
  "is_ongoing": true | false | null,
  "site_owner_is_initiative": true | false | null,
  "notes": string
}

If information is unclear, choose the closest option and explain uncertainty in "notes".
Keep "evidence_quotes" short (≤200 characters each). Use British English.
"""

# ========= Helpers =========
def find_txt_files(base: pathlib.Path) -> List[pathlib.Path]:
    """Find all txt files one level below base (city folders)."""
    return sorted(base.glob("*/*.txt"))

def read_page_sample(p: pathlib.Path, max_chars: int) -> str:
    text = p.read_text(encoding="utf-8", errors="ignore").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars]

def call_classifier(text: str) -> Dict[str, Any]:
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are a precise, concise classifier. Use British English."},
                    {"role": "user", "content": f"{INSTRUCTIONS}\n\n---\nTEXT:\n{text}"}
                ],
                temperature=0.1,
                response_format={"type": "json_object"},
            )
            content = resp.choices[0].message.content
            return json.loads(content)
        except (RateLimitError, APIError, json.JSONDecodeError) as e:
            last_err = e
            # simple backoff
            time.sleep((BACKOFF_BASE ** attempt) + 0.1 * attempt)
    raise RuntimeError(f"Classification failed after {MAX_RETRIES} retries; last error: {last_err}")

# ========= Main =========
def main():
    parser = argparse.ArgumentParser(
        description="Classify scraped FSI website texts using OpenAI."
    )
    parser.add_argument(
        "--txt-base",
        type=pathlib.Path,
        required=True,
        help="Root folder containing city subfolders with .txt files (e.g. Run-03-2ndFiltering/_scraped_text)",
    )
    parser.add_argument(
        "--output-csv",
        type=pathlib.Path,
        required=True,
        help="Path to output CSV file (will be created or overwritten).",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=MAX_CHARS_DEFAULT,
        help=f"Max characters to read from each page (default: {MAX_CHARS_DEFAULT}).",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=PAUSE_DEFAULT,
        help=f"Pause between files in seconds (default: {PAUSE_DEFAULT}).",
    )

    args = parser.parse_args()

    txt_base: pathlib.Path = args.txt_base
    output_csv: pathlib.Path = args.output_csv
    max_chars: int = args.max_chars
    pause_between: float = args.pause

    if not txt_base.exists():
        print(f"❌ Input folder not found: {txt_base}")
        return

    files = find_txt_files(txt_base)
    if not files:
        print(f"No .txt files found under: {txt_base}")
        return

    # ensure output folder exists
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    print(f"Found {len(files)} text files.")
    print(f"Writing results to:\n{output_csv}\n")

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "city",
            "file",
            "url_id",
            "decision",
            "confidence",
            "organisation_name",
            "organisation_type",
            "is_ongoing",
            "site_owner_is_initiative",
            "reasons",
            "evidence_quotes",
            "notes",
        ])

        for p in files:
            city = p.parent.name          # folder name (city)
            url_id = p.stem               # host__hash
            text = read_page_sample(p, max_chars)

            if not text:
                result = {
                    "decision": "exclude",
                    "confidence": 3,
                    "reasons": ["Empty page or no extractable text"],
                    "evidence_quotes": [],
                    "organisation_name": None,
                    "organisation_type": None,
                    "is_ongoing": None,
                    "site_owner_is_initiative": None,
                    "notes": "No content available.",
                }
            else:
                result = call_classifier(text)

            w.writerow([
                city,
                p.name,
                url_id,
                result.get("decision"),
                result.get("confidence"),
                result.get("organisation_name"),
                result.get("organisation_type"),
                result.get("is_ongoing"),
                result.get("site_owner_is_initiative"),
                " | ".join(result.get("reasons", []) or []),
                " | ".join(result.get("evidence_quotes", []) or []),
                result.get("notes", ""),
            ])

            print(f"✓ {city}: {p.name} → {result.get('decision')} (conf {result.get('confidence')})")
            time.sleep(pause_between)

    print("\nDone.")

if __name__ == "__main__":
    main()

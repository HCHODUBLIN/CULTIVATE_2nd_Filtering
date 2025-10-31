import os
import csv
import time
import json
import pathlib
from typing import List, Dict, Any
from openai import OpenAI, RateLimitError, APIError
from dotenv import load_dotenv

load_dotenv()  # .env 로드

# ===== BASE DIRECTORY =====
BASE_DIR = pathlib.Path(__file__).parent

# ========= CONFIG =========
TXT_BASE = BASE_DIR / "Run-03-2ndFiltering" / "_scraped_text"
OUTPUT_CSV = BASE_DIR / "Run-03-2ndFilteringImproved" / "fsi_filter_results_improved.csv"

MODEL = "gpt-4o-mini"      # use 'gpt-4o' if you want maximum quality
MAX_CHARS = 12000          # read up to this many chars per page (most sites fit)
MAX_RETRIES = 5
BACKOFF_BASE = 2.0
PAUSE_BETWEEN_FILES = 0.2

# ========= PROMPT (British English, your exact criteria) =========
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
  "confidence": 1..5,                               // 5 = very confident
  "reasons": [string, ...],                         // concise bullet reasons
  "evidence_quotes": [string, ...],                 // up to 3 short verbatim snippets from the TEXT that support the decision
  "organisation_name": string | null,               // best guess from TEXT
  "organisation_type": "food_bank" | "community_fridge" | "solidarity_kitchen" | "communal_garden" | "seed_library" | "food_club" | "social_supermarket" | "charity" | "cooperative" | "other" | null,
  "is_ongoing": true | false | null,                // based on signals like regular opening hours, recurring activities
  "site_owner_is_initiative": true | false | null,  // does the site appear to be the initiative’s own?
  "notes": string                                   // brief clarifications (British English)
}

If information is unclear, choose the closest option and explain uncertainty in "notes".
Keep "evidence_quotes" short (≤200 characters each). Use British English.
"""

# ========= OpenAI client =========
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError(
        "❌ OPENAI_API_KEY not found. Please create a .env file in this directory containing:\n"
        "OPENAI_API_KEY=sk-your-key-here"
    )

client = OpenAI(api_key=api_key)   # safely reads from .env file
# ========= Helpers =========
def find_txt_files(base: str) -> List[pathlib.Path]:
    # Looks for city subfolders, each containing many *.txt pages
    return sorted(pathlib.Path(base).glob("*/*.txt"))

def read_page_sample(p: pathlib.Path, max_chars: int) -> str:
    text = p.read_text(encoding="utf-8", errors="ignore").strip()
    if len(text) <= max_chars:
        return text
    # take the first max_chars; for very long pages this is usually enough to decide site ownership & purpose
    return text[:max_chars]

def call_classifier(text: str) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Ask for strict JSON output from the Chat Completions API
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
        except (RateLimitError, APIError) as e:
            last_err = e
            time.sleep((BACKOFF_BASE ** attempt) + 0.1 * attempt)
        except json.JSONDecodeError as e:
            # If the model returned something not valid JSON (rare due to response_format), retry once or twice
            last_err = e
            time.sleep((BACKOFF_BASE ** attempt) + 0.1 * attempt)
    raise RuntimeError(f"Classification failed after {MAX_RETRIES} retries; last error: {last_err}")

# ========= Main =========
def main():
    files = find_txt_files(TXT_BASE)
    if not files:
        print(f"No .txt files found under: {TXT_BASE}")
        return

    print(f"Found {len(files)} text files. Writing results to:\n{OUTPUT_CSV}\n")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
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
            "notes"
        ])

        for p in files:
            city = p.parent.name
            url_id = p.stem  # host__hash from your scraping step
            text = read_page_sample(p, MAX_CHARS)

            if not text:
                # Empty page fallback
                result = {
                    "decision": "exclude",
                    "confidence": 3,
                    "reasons": ["Empty page or no extractable text"],
                    "evidence_quotes": [],
                    "organisation_name": None,
                    "organisation_type": None,
                    "is_ongoing": None,
                    "site_owner_is_initiative": None,
                    "notes": "No content available."
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
                result.get("notes", "")
            ])

            print(f"✓ {city}: {p.name} → {result.get('decision')} (conf {result.get('confidence')})")
            time.sleep(PAUSE_BETWEEN_FILES)

    print("\nDone.")

if __name__ == "__main__":
    main()

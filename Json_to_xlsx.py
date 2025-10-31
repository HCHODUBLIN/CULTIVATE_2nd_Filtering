import os
import json
import pathlib
import pandas as pd
from pandas import json_normalize

# ====== CONFIG ======

BASE_DIR = pathlib.Path(__file__).parent / "Run-04" / "01--to-process"

OUTPUT_SUFFIX = "_results.xlsx"   # 파이프라인 호환 (City_results.xlsx 형태)

# URL 후보 컬럼 이름(대소문자 무시)
URL_CANDIDATES = [
    "url", "website", "link", "homepage", "web", "site", "page_url"
]

def find_inputs(base: str):
    exts = (".json", ".jsonl", ".ndjson")
    return sorted([p for p in pathlib.Path(base).glob("*") if p.suffix.lower() in exts])

def load_json_any(path: pathlib.Path):
    """JSON/NDJSON 모두 처리."""
    try:
        # NDJSON/JSONL 우선 시도
        return pd.read_json(path, lines=True)
    except Exception:
        pass
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    # list → 테이블
    if isinstance(raw, list):
        return pd.DataFrame(raw)
    # dict → 흔한 키(data/items/rows/results) 우선
    if isinstance(raw, dict):
        for key in ("data", "items", "rows", "results", "records"):
            if key in raw and isinstance(raw[key], list):
                return pd.DataFrame(raw[key])
        # 그 외엔 평탄화
        flat = json_normalize(raw, sep=".")
        return flat
    # 기타 타입은 문자열로 보존
    return pd.DataFrame([{"_value": raw}])

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    # 공백·개행 제거, 양끝 공백 제거
    df.columns = [str(c).strip().replace("\n", " ").replace("\r", " ") for c in df.columns]
    return df

def promote_url_column(df: pd.DataFrame) -> pd.DataFrame:
    """URL 후보 컬럼을 탐지해 'URL'로 리네임(없으면 그대로)."""
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}

    # 1) 이미 'URL'이 있으면 그대로
    if "URL" in cols:
        return df

    # 2) 후보 탐색(대소문자 무시)
    for cand in URL_CANDIDATES:
        if cand in lower_map:
            src = lower_map[cand]
            if src != "URL":
                # URL 컬럼이 없을 때만 리네임
                df = df.rename(columns={src: "URL"})
            return df

    # 3) 중첩 칼럼명에 url 포함되는 경우 (예: 'contact.url', 'links.website')
    for c in cols:
        if "url" in c.lower():
            df = df.rename(columns={c: "URL"})
            return df

    # 못 찾으면 그대로
    return df

def to_excel_safe(df: pd.DataFrame, out_path: pathlib.Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # 너무 넓은 DF의 폭을 줄이기 위해 object 열은 32, 나머지는 기본
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="Sheet1")

def process_file(path: pathlib.Path):
    df = load_json_any(path)
    df = normalise_columns(df)
    df = promote_url_column(df)

    # 파일명 → *_results.xlsx
    stem = path.stem
    out_name = f"{stem}{OUTPUT_SUFFIX}" if not stem.endswith("_results") else f"{stem}.xlsx"
    out_path = path.with_name(out_name)

    to_excel_safe(df, out_path)
    print(f"✓ {path.name} → {out_path.name}  (rows: {len(df)})")

def main():
    inputs = find_inputs(BASE_DIR)
    if not inputs:
        print(f"No JSON/NDJSON files found in {BASE_DIR}")
        return
    print(f"Found {len(inputs)} file(s) in {BASE_DIR}")
    for p in inputs:
        try:
            process_file(p)
        except Exception as e:
            print(f"[ERROR] {p.name}: {e}")

if __name__ == "__main__":
    main()

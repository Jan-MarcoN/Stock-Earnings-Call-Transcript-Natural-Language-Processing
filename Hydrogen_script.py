import datetime as dt
from pathlib import Path
import pandas as pd
import nltk
import json
import random
from nltk.tokenize import sent_tokenize

nltk.download("punkt")

def load_dict(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# Load dictionaries
hydrogen_dict = load_dict(Path("dictionaries/hydrogen_keywords.json"))
hydrogen_keywords = set(kw.lower() for sublist in hydrogen_dict.values() for kw in sublist)


# Read metadata
metadata = pd.read_csv("earnings_to_search.csv")

# Sample 100 random transcripts
sampled = metadata.sample(n=min(100, len(metadata)), random_state=42)

results = []

for _, row in sampled.iterrows():
    article_path = f'.{row["article"]}.txt'
    try:
        with open(article_path, "r", encoding="utf-8") as f:
            text = f.read()

        sentences = sent_tokenize(text)
        for sentence in sentences:
            lower_sentence = sentence.lower()
            matched_hydrogen = [kw for kw in hydrogen_keywords if kw in lower_sentence]
            if matched_hydrogen:
                results.append({
                    "company": row["ticker"],
                    "date": row["date"],
                    "keywords": ", ".join(matched_hydrogen),
                    "sentence": sentence.strip()
                })

    except Exception as e:
        print(f"Error processing {article_path}: {e}")

# Save results to Excel
OUT_ROOT = Path("outputs")
output_dir = OUT_ROOT / str(dt.date.today())
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / f"hydrogen_matches_{dt.date.today()}.xlsx"
pd.DataFrame(results).to_excel(output_path, index=False)
print(f"Saved {len(results)} matched sentences to {output_path}")
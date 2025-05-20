import datetime as dt
from pathlib import Path
import pandas as pd
import nltk
import json
import random
from nltk.tokenize import sent_tokenize
import sys

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
# sampled = metadata.sample(n=min(100, len(metadata)), random_state=42)
sampled = metadata

results = []

for idx, row in sampled.iterrows():
    article_path = f'.{row["article"]}.txt'
    try:
        with open(article_path, "r", encoding="utf-8") as f:
            full_text = f.read()
            parts = full_text.split("question-and-answer session")
            prepared_text = parts[0]
            qa_text = parts[1] if len(parts) > 1 else ""

        for section_name, text in [("prepared", prepared_text), ("qa", qa_text)]:
            sentences = sent_tokenize(text)
            for i, sentence in enumerate(sentences):
                context_before = " ".join(sentences[max(i - 2, 0):i])
                context_after = " ".join(sentences[i + 1:i + 4])
                # Only match hydrogen keywords in the main sentence
                matched_hydrogen = [kw for kw in hydrogen_keywords if kw in sentence.lower()]

                if matched_hydrogen:
                    results.append({
                        "company": row["ticker"],
                        "date": row["date"],
                        "section": section_name,
                        "keywords": ", ".join(matched_hydrogen),
                        "sentence": sentence.strip(),
                        "context_before": context_before.strip(),
                        "context_after": context_after.strip()
                    })

    except Exception as e:
        print(f"Error processing {article_path}: {e}")

# Save results to Excel
OUT_ROOT = Path("outputs")
output_dir = OUT_ROOT / str(dt.date.today())
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / f"preprocessed_sentences_{dt.date.today()}.xlsx"
df_results = pd.DataFrame(results)
# Ensure columns order for Excel output
columns = [
    "company",
    "date",
    "section",
    "keywords",
    "sentence",
    "context_before",
    "context_after"
]
df_results = df_results[columns]
df_results.to_excel(output_path, index=False)
print(f"Saved {len(results)} matched sentences to {output_path}")

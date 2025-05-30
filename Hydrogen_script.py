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

uncertainty_dict = load_dict(Path("dictionaries/uncertainty_keywords.json"))
general_uncertainty_keywords = set(kw.lower() for kw in uncertainty_dict["uncertainty_general"])
uncertainty_subtypes = {
    subtype: set(kw.lower() for kw in keywords)
    for subtype, keywords in uncertainty_dict["uncertainty_subtypes"].items()
}

framing_dict = load_dict(Path("dictionaries/framing_keywords.json"))
# Load orchestration keyword dictionary
orchestration_keywords = load_dict(Path("dictionaries/orchestration_keywords.json"))

# Read metadata
metadata = pd.read_csv("earnings_to_search.csv")

# Sample 100 random transcripts
sampled = metadata.sample(n=min(100, len(metadata)), random_state=42)
# sampled = metadata

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
                combined_text = " ".join(sentences[max(i - 2, 0):i + 4]).lower()
                # Only match hydrogen keywords in the main sentence
                matched_hydrogen = [kw for kw in hydrogen_keywords if kw in sentence.lower()]
                if matched_hydrogen:
                    matched_uncertainty_general = [kw for kw in general_uncertainty_keywords if kw in combined_text]
                    matched_certainty = [kw for kw in uncertainty_dict["certainty"] if kw.lower() in combined_text]
                    matched_uncertainty_subtypes = {
                        subtype: [kw for kw in kws if kw in combined_text]
                        for subtype, kws in uncertainty_subtypes.items()
                    }
                    matched_subtypes = [s for s, hits in matched_uncertainty_subtypes.items() if hits]

                    matched_framing = []
                    for category, framing_words in framing_dict.items():
                        if any(fw in combined_text for fw in framing_words):
                            matched_framing.append(category)
                    # Orchestration/coordination terms
                    matched_coordination_terms = [kw for kw in orchestration_keywords if kw in combined_text]

                    results.append({
                        "company": row["ticker"],
                        "date": row["date"],
                        "section": section_name,
                        "keywords": ", ".join(matched_hydrogen),
                        # "certainty_terms_text": ", ".join(matched_certainty),
                        "certainty_terms": len(matched_certainty),
                        # "uncertainty_general_text": ", ".join(matched_uncertainty_general),
                        "uncertainty_general": len(matched_uncertainty_general),
                        # "technological_text": ", ".join(matched_uncertainty_subtypes.get("technological", [])),
                        "technological": len(matched_uncertainty_subtypes.get("technological", [])),
                        # "market_application_text": ", ".join(matched_uncertainty_subtypes.get("market_application", [])),
                        "market_application": len(matched_uncertainty_subtypes.get("market_application", [])),
                        # "ecosystem_text": ", ".join(matched_uncertainty_subtypes.get("ecosystem", [])),
                        "ecosystem": len(matched_uncertainty_subtypes.get("ecosystem", [])),
                        # "business_model_text": ", ".join(matched_uncertainty_subtypes.get("business_model", [])),
                        "business_model": len(matched_uncertainty_subtypes.get("business_model", [])),
                        # "interaction_text": ", ".join(matched_uncertainty_subtypes.get("interaction", [])),
                        "interaction": len(matched_uncertainty_subtypes.get("interaction", [])),
                        # "uncertainty_subtypes_text": ", ".join(matched_subtypes),
                        "uncertainty_subtypes": len(matched_subtypes),
                        "framing": ", ".join(matched_framing),
                        "framing_count": len(matched_framing),
                        "coordination_terms": ", ".join(matched_coordination_terms),
                        "coordination_term_count": len(matched_coordination_terms),
                        "sentence": sentence.strip(),
                        "context_before": context_before.strip(),
                        "context_after": context_after.strip()
                    })

    except Exception as e:
        print(f"Error processing {article_path}: {e}")

# Save results to Excel
OUT_ROOT = Path("outputs")
output_dir = OUT_ROOT / "full_{dt.date.today()}"
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / f"hydrogen_matches_{dt.date.today()}.xlsx"
df_results = pd.DataFrame(results)
# Ensure columns order for Excel output
columns = [
    "company",
    "date",
    "section",
    "keywords",
    "certainty_terms",
    "uncertainty_general",
    "uncertainty_subtypes",
    "technological",
    "market_application",
    "ecosystem",
    "business_model",
    "interaction",
    "framing",
    "framing_count",
    "coordination_terms",
    "coordination_term_count",
    "sentence",
    "context_before",
    "context_after"
]
df_results = df_results[columns]
df_results.to_excel(output_path, index=False)
print(f"Saved {len(results)} matched sentences to {output_path}")

# Filter: co-occurrence of ecosystem uncertainty and coordination terms
df_filtered = df_results[(df_results["ecosystem"] > 0) & (df_results["coordination_term_count"] > 0)]

# Save filtered co-occurrence subset
filtered_output_path = output_dir / f"cooccurrence_ecosystem_coordination_{dt.date.today()}.xlsx"
df_filtered.to_excel(filtered_output_path, index=False)
print(f"Saved {len(df_filtered)} co-occurrence rows to {filtered_output_path}")

# Filter: framing of uncertainty in coordination context
df_framing_coord = df_results[
    (df_results["coordination_term_count"] > 0) &
    (df_results["framing"].str.contains("opportunity|risk", case=False, na=False))
]

# Save filtered framing vs. coordination subset
framing_coord_output_path = output_dir / f"framing_coordination_{dt.date.today()}.xlsx"
df_framing_coord.to_excel(framing_coord_output_path, index=False)
print(f"Saved {len(df_framing_coord)} framing-coordination rows to {framing_coord_output_path}")
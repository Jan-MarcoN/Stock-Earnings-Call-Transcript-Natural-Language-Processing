

import os
import json
from pathlib import Path
import nltk
from transformers import pipeline
from utils import load_dict

nltk.download("punkt")

# Load dictionaries
hydrogen_dict = load_dict(Path("dictionaries/hydrogen_keywords.json"))
uncertainty_dict = load_dict(Path("dictionaries/uncertainty_keywords.json"))

hydrogen_keywords = sum(hydrogen_dict.values(), [])
uncertainty_labels = uncertainty_dict["uncertainty"]

# Load classifier
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

def load_transcripts(folder="article"):
    for path in Path(folder).glob("*.txt"):
        yield {
            "company": path.stem.split("_")[0],
            "date": path.stem.split("_")[1],
            "content": path.read_text(encoding="utf-8")
        }

def extract_hydrogen_sentences(text, keywords):
    sentences = nltk.sent_tokenize(text)
    return [s for s in sentences if any(kw.lower() in s.lower() for kw in keywords)]

def classify_uncertainty(sentence, labels):
    out = classifier(sentence, labels)
    best = out["labels"][0] if out["scores"][0] > 0.4 else None
    return best

def process_transcript(record):
    content = record["content"]
    hydrogen_sentences = extract_hydrogen_sentences(content, hydrogen_keywords)
    classified = [(s, classify_uncertainty(s, uncertainty_labels)) for s in hydrogen_sentences]

    result = {
        "company": record["company"],
        "date": record["date"],
        "hydrogen_mentions": len(hydrogen_sentences)
    }

    for label in uncertainty_labels:
        result[label] = sum(1 for _, u in classified if u == label)

    return result

def main():
    all_results = []
    for record in load_transcripts():
        result = process_transcript(record)
        if result["hydrogen_mentions"] > 0:
            all_results.append(result)

    outpath = Path("outputs/hydrogen_uncertainty_scores.json")
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"Saved {len(all_results)} results to {outpath}")

if __name__ == "__main__":
    main()
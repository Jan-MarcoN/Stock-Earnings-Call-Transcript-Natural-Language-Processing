

import os
import json
from pathlib import Path
import nltk
from transformers import pipeline
import random
import json
from pathlib import Path

def load_dict(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

nltk.download("punkt")

# Load dictionaries
hydrogen_dict = load_dict(Path("/Users/jan-marconepute/PycharmProjects/Stock-Earnings-Call-Transcript-Natural-Language-Processing/dictionaries/hydrogen_keywords.json"))
uncertainty_dict = load_dict(Path("/Users/jan-marconepute/PycharmProjects/Stock-Earnings-Call-Transcript-Natural-Language-Processing/dictionaries/uncertainty_keywords.json"))

hydrogen_keywords = sum(hydrogen_dict.values(), [])
uncertainty_labels = [
    phrase for phrases in uncertainty_dict.get("uncertainty_subtypes", {}).values() for phrase in phrases
]

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
    import random
    all_results = []
    transcripts = list(load_transcripts())
    sampled = random.sample(transcripts, min(1000, len(transcripts)))
    for record in sampled:
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
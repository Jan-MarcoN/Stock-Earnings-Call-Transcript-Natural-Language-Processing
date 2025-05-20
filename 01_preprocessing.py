import datetime as dt
from pathlib import Path
import pandas as pd
import nltk
import json
import random
from nltk.tokenize import sent_tokenize
import sys

# === SCRIPT TO EXTRACT HYDROGEN-RELATED SENTENCES FROM TRANSCRIPTS
#     AND APPEND STOCK PRICES ON EARNINGS DATES ===

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


# === LOAD EARNINGS CALL METADATA ===
# Load the earnings call dates
# This CSV should contain at least two columns: 'ticker' and 'date'
earnings_df = pd.read_csv("earnings_to_search.csv")
earnings_df["date"] = pd.to_datetime(earnings_df["date"])

# === FUNCTION TO MATCH EARNINGS-DAY PRICES WITH TICKER + DATE ===
def process_stock_folder(folder_path):
    # Create an output directory using today's date
    output_dir = Path("outputs") / str(dt.date.today())
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "stock_price_summary.xlsx"

    folder = Path(folder_path)
    all_prices = []

    # Process each CSV file in the folder
    for file in folder.glob("*.csv"):
        try:
            df = pd.read_csv(file)
            df["Date"] = pd.to_datetime(df["Date"])
            ticker = file.stem  # Use filename (without .csv) as the ticker symbol
            df["Ticker"] = ticker

            # Get all earnings call dates for this ticker
            ticker_df = earnings_df[earnings_df["ticker"] == ticker]
            if ticker_df.empty:
                print(f"No earnings dates found for {ticker}, skipping.")
                continue

            # Merge stock prices with earnings dates for this ticker
            df_merged = pd.merge(
                ticker_df,
                df[["Date", "Adj Close"]],
                left_on="date",
                right_on="Date",
                how="left"
            )

            # Select relevant columns and rename for clarity
            df_merged = df_merged[["ticker", "date", "Adj Close"]].rename(columns={"Adj Close": "Earnings_Day_Price"})
            all_prices.append(df_merged)
        except Exception as e:
            print(f"Failed to process {file.name}: {e}")

    # Concatenate and export the final stock price summary
    if all_prices:
        df_prices = pd.concat(all_prices, ignore_index=True)
        df_prices.to_excel(output_file, index=False)
        print(f"✅ Saved earnings day stock prices for all stocks to {output_file}")

        # === RENAME COLUMNS FOR MERGING ===
        df_prices = df_prices.rename(columns={"ticker": "company", "date": "price_date"})

        # === MERGE PREPROCESSED SENTENCES WITH EARNINGS-DAY STOCK PRICES ===
        try:
            # Load preprocessed hydrogen sentence dataset
            df_preprocessed = pd.read_excel(output_path)
            df_preprocessed["date"] = pd.to_datetime(df_preprocessed["date"])

            df_prices = df_prices.rename(columns={"ticker": "company", "date": "price_date"})

            # Merge preprocessed data with matched stock prices
            df_merged_sentences = pd.merge(
                df_preprocessed,
                df_prices,
                left_on=["company", "date"],
                right_on=["company", "price_date"],
                how="left"
            )

            # Save the merged output
            merged_output_path = output_dir / "preprocessed_with_prices.xlsx"
            df_merged_sentences.to_excel(merged_output_path, index=False)
            print(f"✅ Merged preprocessing data with stock prices saved to {merged_output_path}")
        except Exception as e:
            print(f"❌ Failed to merge stock prices with preprocessed data: {e}")
    else:
        print("No valid CSV files found.")

# Run the function on the 'stock_price' folder
process_stock_folder("stock_price")

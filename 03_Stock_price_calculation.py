import pandas as pd
from pathlib import Path
import datetime as dt

# Load the earnings call dates
# This CSV should contain at least two columns: 'ticker' and 'date'
earnings_df = pd.read_csv("earnings_to_search.csv")
earnings_df["date"] = pd.to_datetime(earnings_df["date"])

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
        df_combined = pd.concat(all_prices, ignore_index=True)
        df_combined.to_excel(output_file, index=False)
        print(f"✅ Saved earnings day stock prices for all stocks to {output_file}")

        try:
            # Load preprocessed hydrogen sentence dataset
            df_preprocessed = pd.read_excel("outputs/2025-05-20/preprocessed_sentences_2025-05-20.xlsx")
            df_preprocessed["date"] = pd.to_datetime(df_preprocessed["date"])

            # Merge preprocessed data with matched stock prices
            df_merged_all = pd.merge(
                df_preprocessed,
                df_combined.rename(columns={"ticker": "company", "date": "price_date"}),
                left_on=["company", "date"],
                right_on=["company", "price_date"],
                how="left"
            )

            # Save the merged output
            merged_output_path = output_dir / "preprocessed_with_prices.xlsx"
            df_merged_all.to_excel(merged_output_path, index=False)
            print(f"✅ Merged preprocessing data with stock prices saved to {merged_output_path}")
        except Exception as e:
            print(f"❌ Failed to merge stock prices with preprocessed data: {e}")
    else:
        print("No valid CSV files found.")

# Run the function on the 'stock_price' folder
process_stock_folder("stock_price")

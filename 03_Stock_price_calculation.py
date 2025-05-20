import pandas as pd
# Load the earnings call dates
earnings_df = pd.read_csv("earnings_to_search.csv")
earnings_df["date"] = pd.to_datetime(earnings_df["date"])

# Function to process a folder of stock CSVs and aggregate monthly mean prices
from pathlib import Path
import datetime as dt

def process_stock_folder(folder_path):
    output_dir = Path("outputs") / str(dt.date.today())
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "stock_price_summary.xlsx"
    folder = Path(folder_path)
    all_monthly = []

    for file in folder.glob("*.csv"):
        try:
            df = pd.read_csv(file)
            df["Date"] = pd.to_datetime(df["Date"])
            ticker = file.stem  # assume file name is the ticker symbol
            df["Ticker"] = ticker
            ticker_df = earnings_df[earnings_df["ticker"] == ticker]
            if ticker_df.empty:
                print(f"No earnings dates found for {ticker}, skipping.")
                continue
            df_merged = pd.merge(
                ticker_df,
                df[["Date", "Adj Close"]],
                left_on="date",
                right_on="Date",
                how="left"
            )
            df_merged = df_merged[["ticker", "date", "Adj Close"]].rename(columns={"Adj Close": "Earnings_Day_Price"})
            all_monthly.append(df_merged)
        except Exception as e:
            print(f"Failed to process {file.name}: {e}")

    if all_monthly:
        df_combined = pd.concat(all_monthly, ignore_index=True)
        df_combined.to_excel(output_file, index=False)
        print(f"✅ Saved earnings day stock prices for all stocks to {output_file}")

        try:import pandas as pd
# Load the earnings call dates
earnings_df = pd.read_csv("earnings_to_search.csv")
earnings_df["date"] = pd.to_datetime(earnings_df["date"])

# Function to process a folder of stock CSVs and aggregate monthly mean prices
from pathlib import Path
import datetime as dt

def process_stock_folder(folder_path):
    output_dir = Path("outputs") / str(dt.date.today())
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "stock_price_summary.xlsx"
    folder = Path(folder_path)
    all_monthly = []

    for file in folder.glob("*.csv"):
        try:
            df = pd.read_csv(file)
            df["Date"] = pd.to_datetime(df["Date"])
            ticker = file.stem  # assume file name is the ticker symbol
            df["Ticker"] = ticker
            ticker_df = earnings_df[earnings_df["ticker"] == ticker]
            if ticker_df.empty:
                print(f"No earnings dates found for {ticker}, skipping.")
                continue
            df_merged = pd.merge(
                ticker_df,
                df[["Date", "Adj Close"]],
                left_on="date",
                right_on="Date",
                how="left"
            )
            df_merged = df_merged[["ticker", "date", "Adj Close"]].rename(columns={"Adj Close": "Earnings_Day_Price"})
            all_monthly.append(df_merged)
        except Exception as e:
            print(f"Failed to process {file.name}: {e}")

    if all_monthly:
        df_combined = pd.concat(all_monthly, ignore_index=True)
        df_combined.to_excel(output_file, index=False)
        print(f"✅ Saved earnings day stock prices for all stocks to {output_file}")

        try:
            # Load the preprocessing dataset
            df_preprocessed = pd.read_excel("outputs/2025-05-20/preprocessed_sentences_2025-05-20.xlsx")
            df_preprocessed["date"] = pd.to_datetime(df_preprocessed["date"])

            # Merge on ticker and date
            df_merged_all = pd.merge(
                df_preprocessed,
                df_combined.rename(columns={"ticker": "company", "date": "price_date"}),
                left_on=["company", "date"],
                right_on=["company", "price_date"],
                how="left"
            )

            # Save the merged dataset
            merged_output_path = output_dir / "preprocessed_with_prices.xlsx"
            df_merged_all.to_excel(merged_output_path, index=False)
            print(f"✅ Merged preprocessing data with stock prices saved to {merged_output_path}")
        except Exception as e:
            print(f"❌ Failed to merge stock prices with preprocessed data: {e}")
    else:
        print("No valid CSV files found.")

# Example usage:
process_stock_folder("stock_price")
            # Load the preprocessing dataset
            df_preprocessed = pd.read_excel("outputs/2025-05-20/preprocessed_sentences_2025-05-20.xlsx")
            df_preprocessed["date"] = pd.to_datetime(df_preprocessed["date"])

            # Merge on ticker and date
            df_merged_all = pd.merge(
                df_preprocessed,
                df_combined.rename(columns={"ticker": "company", "date": "price_date"}),
                left_on=["company", "date"],
                right_on=["company", "price_date"],
                how="left"
            )

            # Save the merged dataset
            merged_output_path = output_dir / "preprocessed_with_prices.xlsx"
            df_merged_all.to_excel(merged_output_path, index=False)
            print(f"✅ Merged preprocessing data with stock prices saved to {merged_output_path}")
        except Exception as e:
            print(f"❌ Failed to merge stock prices with preprocessed data: {e}")
    else:
        print("No valid CSV files found.")

# Example usage:
process_stock_folder("stock_price")
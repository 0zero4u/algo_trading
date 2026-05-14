import os
import urllib.request
import zipfile
import logging
import polars as pl

SYMBOLS = ["SOLUSDT", "BTCUSDT"]
MONTH = "2025-07"
BASE_URL = "https://data.binance.vision/data/futures/um/monthly/trades"

def download_and_convert():
    print(f"\n--- STEP 0: Ingesting & Optimizing Raw Data ({MONTH}) ---")
    for symbol in SYMBOLS:
        zip_file = f"{symbol}-trades-{MONTH}.zip"
        csv_file = f"{symbol}-trades-{MONTH}.csv"
        parquet_file = f"{symbol}-trades-{MONTH}.parquet"

        if os.path.exists(parquet_file):
            print(f"✅ {symbol} Parquet found locally. Skipping.")
            continue

        url = f"{BASE_URL}/{symbol}/{zip_file}"
        try:
            urllib.request.urlretrieve(url, zip_file)
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall()

            print(f"📦 Converting {csv_file} to optimized Parquet...")
            
            # 1. Read the CSV (Polars automatically handles headers and scientific notation)
            df = pl.read_csv(csv_file)

            # 2. Cast the timestamp to Datetime("ms") immediately for native storage
            df = df.with_columns(
                pl.col("time").cast(pl.Datetime("ms"))
            )

            # 3. Write to Snappy compressed Parquet
            df.write_parquet(parquet_file, compression="snappy")

            os.remove(zip_file)
            os.remove(csv_file)
        except Exception as e:
            logging.error(f"❌ Failed to process {symbol}. Error: {e}")

if __name__ == "__main__":
    download_and_convert()

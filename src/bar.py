# ==========================================
# COLAB CELL 3: LAZY 10-DAY CHUNKED BAR GENERATION (2.0.1.py)
# ==========================================
import polars as pl
import gc
from ml4t.specs import MarketDataSpec, read_spec_payload
from ml4t.engineer.bars import DollarBarSampler
from ml4t.data.storage import HiveStorage, StorageConfig
from ml4t.data.validation import OHLCVValidator
from ml4t.data.validation.rules import ValidationRulePresets

def generate_bars_chunked():
    # Load the global contract to ensure strict compliance
    spec = MarketDataSpec.from_mapping(read_spec_payload("solusdt_contract.yaml"))

    print("--- STEP 1: Initializing Lazy Streams ---")
    # Include 'id' here to perfectly preserve state across chunk boundaries
    sol_lazy = pl.scan_parquet("SOLUSDT-trades-2025-07.parquet").select([
        pl.col("id"),
        pl.col("time").alias("timestamp"),
        pl.col("price"),
        pl.col("qty").alias("volume")
    ])
    
    btc_lazy = pl.scan_parquet("BTCUSDT-trades-2025-07.parquet").select([
        pl.col("time").alias("timestamp"),
        pl.col("price").alias("btc_close")
    ])

    # Instantiate the sampler once
    sampler = DollarBarSampler(dollars_per_bar=3_500_000)
    
    leftover_state = None
    all_synchronized_bars = []

    print("--- STEP 2: Stateful 10-Day Chunking ---")
    # Iterate through the month in 10-day chunks (1-10, 11-20, 21-30, 31)
    for start_day in range(1, 32, 10):
        end_day = min(start_day + 9, 31)
        print(f"📦 Processing Days {start_day} to {end_day}...")
        
        # Pull the 10-day chunk into RAM
        sol_chunk = sol_lazy.filter(
            (pl.col("timestamp").dt.day() >= start_day) & 
            (pl.col("timestamp").dt.day() <= end_day)
        ).collect()
        
        if sol_chunk.is_empty():
            continue

        # Prepend unclosed volume from the previous chunk to maintain dollar-bar state
        if leftover_state is not None and not leftover_state.is_empty():
            sol_chunk = pl.concat([leftover_state, sol_chunk])

        # Generate Bars for this 10-day block
        sol_bars = sampler.sample(sol_chunk)

        # If volume was too low to generate even one bar, carry the whole chunk over
        if sol_bars.is_empty():
            leftover_state = sol_chunk
            continue

        # --- Capture leftover ticks perfectly using Binance Trade ID ---
        # 1. Get the exact millisecond the last bar closed
        last_bar_time = sol_bars["timestamp"].max()
        
        # 2. Find the maximum sequential Trade ID that occurred on or before that millisecond
        last_tick_in_bar = sol_chunk.filter(
            pl.col("timestamp") <= last_bar_time
        ).sort("id").tail(1)
        
        if last_tick_in_bar.height > 0:
            last_trade_id = last_tick_in_bar["id"]
            # 3. Everything strictly after this Trade ID is leftover state for the next chunk
            leftover_state = sol_chunk.filter(pl.col("id") > last_trade_id)
        else:
            leftover_state = sol_chunk

        # --- Synchronize with BTC for this chunk ---
        # Add a 1-hour buffer to ensure the join_asof has data if a bar closes exactly at midnight
        chunk_start = sol_chunk["timestamp"].min() - pl.duration(hours=1)
        chunk_end = sol_chunk["timestamp"].max()

        btc_chunk = btc_lazy.filter(
            (pl.col("timestamp") >= chunk_start) & 
            (pl.col("timestamp") <= chunk_end)
        ).sort("timestamp").collect()

        # Bind BTC macro price to the exact millisecond the SOL bar closed
        sync_chunk = sol_bars.join_asof(
            btc_chunk,
            on="timestamp",
            strategy="backward"
        ).with_columns(pl.lit("SOLUSDT").alias(spec.schema.entity_col))

        # Drop overlapping millisecond bars caused by massive sweep orders
        sync_chunk = sync_chunk.unique(subset=["timestamp"], keep="last", maintain_order=True)
        all_synchronized_bars.append(sync_chunk)

        # Aggressively clear RAM for the next 10-day loop
        del sol_chunk, sol_bars, btc_chunk, sync_chunk
        gc.collect()

    print("--- STEP 3: Reassembling & Validating ---")
    if not all_synchronized_bars:
        raise ValueError("No bars were generated across the entire dataset. Adjust your dollars_per_bar threshold.")
        
    combined_df = pl.concat(all_synchronized_bars)
    print(f"📊 Final Synchronized Dataframe shape: {combined_df.shape}")

    print("--- SYNCHRONIZED DATAFRAME PREVIEW ---")
    print(combined_df.head(5))
    print("\nColumns:", combined_df.columns)
    print("\nSchema:")
    print(combined_df.schema)

    print("--- STEP 4: Diagnostic Validation ---")
    # Apply the crypto-specific rules to accommodate high volatility
    crypto_rules = ValidationRulePresets.crypto_rules()
    validator = OHLCVValidator(
        max_return_threshold=crypto_rules.max_return_threshold,
        staleness_threshold=crypto_rules.staleness_threshold
    )

    val_result = validator.validate(combined_df)
    error_issues = [issue for issue in val_result.issues if "ERROR" in str(issue.severity)]

    if error_issues:
        print("\n❌ VALIDATION FAILED! EXACT REASONS:")
        for issue in error_issues:
            print(f"  -> {issue.check}: {issue.message}")

        print("\nDataframe Head:")
        print(combined_df.head(3))
        raise ValueError("Pipeline stopped due to schema errors.")
    else:
        print("✅ ML4T Schema & Crypto Validation Passed!")

    print("--- STEP 5: Storing in Parquet Hive ---")
    storage = HiveStorage(StorageConfig(base_path=str(spec.storage.path), strategy="hive"))
    storage.write(combined_df, "crypto_bars_SOLUSDT")
    print("🚀 Synchronized Bars cleanly written to ML4T Hive.")

# Execute the entire function
generate_bars_chunked()

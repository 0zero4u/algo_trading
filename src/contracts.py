# ==========================================
# COLAB CELL 1: THE GLOBAL CONTRACT (001.py)
# ==========================================
from ml4t.specs import (
    MarketDataSchema,
    MarketDataSemantics,
    ArtifactStorage,
    MarketDataSpec,
    write_spec_payload
)

def build_and_save_contract():
    print("--- Generating ML4T Global Contract ---")

    # 1. Define the Spec (Taking advantage of ML4T defaults!)
    spec = MarketDataSpec(
        artifact_id="solusdt_dollar_bars_v1",

        # Define where and how the data is stored
        storage=ArtifactStorage(
            path="./ml4t_data/",              # Base path used by HiveStorage
            format="parquet",
            partition_by=("month",)           # Matches your 2.0.1.py config
        ),

        # Define the column rules.
        # (timestamp and close are defaults, but we explicitly set entity_col)
        schema=MarketDataSchema(
            timestamp_col="timestamp",
            entity_col="asset_id",            # Matches your 2.6.py script
            close_col="close",
            price_col="close"
        ),

        # Define the market rules
        semantics=MarketDataSemantics(
            calendar="24/7",
            timezone="UTC",
            data_frequency="dollar_bar"
        )
    )

    # 2. Write to YAML using .to_dict() as required by the library's internal tests
    contract_path = "solusdt_contract.yaml"
    write_spec_payload(spec.to_dict(), contract_path)

    print(f"✅ Global Contract successfully saved to: {contract_path}")

# Run it
build_and_save_contract()

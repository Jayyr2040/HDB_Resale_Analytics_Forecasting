import os
import sys

from dagster import asset, MaterializeResult

# inject root folder path into the active python runtime path context
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from scripts.enrich import run_enrichment_pipeline

@asset(
    name="raw_enriched_transactions",
    compute_kind="python",
    group_name="ingestion"
)

def raw_enriched_transactions_asset() -> MaterializeResult:

    """
    Triggers custom pipeline logic inside scripts/enrich.py 
    """
    total_processed_rows = run_enrichment_pipeline()
    return MaterializeResult(
        metadata = {
            "status": "Pipeline execution successful",
            "schema_target": "hdb_raw_staging.raw_enriched_transactions",
            "records_staged": total_processed_rows if total_processed_rows else "Dynamic/Unknown"
        }
    )
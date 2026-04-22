"""BigQuery download helper — uses Storage Read API via basedosdados.

basedosdados already supports use_bqstorage_api=True which routes downloads
through the Arrow/gRPC Storage API (5–10× faster than REST pagination).
Auth is still handled by basedosdados's pydata_google_auth OAuth flow so no
extra credential setup is needed.

Requires: google-cloud-bigquery-storage (see requirements.txt)
"""
import basedosdados as bd
import pandas as pd


def bq_warmup_auth(billing_project_id: str) -> None:
    """Run a trivial query in the main thread to cache OAuth credentials
    before parallel workers start, preventing concurrent auth prompts."""
    bd.read_sql("SELECT 1 AS ok", billing_project_id=billing_project_id)


def bq_to_df(query: str, billing_project_id: str) -> pd.DataFrame:
    return bd.read_sql(
        query,
        billing_project_id=billing_project_id,
        use_bqstorage_api=True,
    )

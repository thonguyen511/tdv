"""
TradingView Distributed Data Pipeline (`tdv`).

Two-phase (Initial Full Scan + Weekly Maintenance), two-stage (`1D` discovery +
`has_intraday`-filtered intraday & macro intervals) encrypted Parquet pipeline
backed by Hugging Face Storage Buckets and orchestrated across a 20-node Kaggle
fleet + GitHub Actions.
"""

__version__ = "2.0.0"


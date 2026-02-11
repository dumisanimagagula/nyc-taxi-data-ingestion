import pandas as pd
import pytest

from ingest_nyc_taxi_data import validate_chunk


def _sample_chunk(**overrides) -> pd.DataFrame:
    """Create a minimal valid chunk with optional column overrides."""
    base = {
        "VendorID": pd.Series([1], dtype="Int64"),
        "passenger_count": pd.Series([2], dtype="Int64"),
        "trip_distance": pd.Series([3.5], dtype="float64"),
        "RatecodeID": pd.Series([1], dtype="Int64"),
        "store_and_fwd_flag": pd.Series(["N"], dtype="string"),
        "PULocationID": pd.Series([10], dtype="Int64"),
        "DOLocationID": pd.Series([20], dtype="Int64"),
        "payment_type": pd.Series([1], dtype="Int64"),
        "fare_amount": pd.Series([12.5], dtype="float64"),
        "extra": pd.Series([0.5], dtype="float64"),
        "mta_tax": pd.Series([0.5], dtype="float64"),
        "tip_amount": pd.Series([3.0], dtype="float64"),
        "tolls_amount": pd.Series([0.0], dtype="float64"),
        "improvement_surcharge": pd.Series([0.3], dtype="float64"),
        "total_amount": pd.Series([16.3], dtype="float64"),
        "congestion_surcharge": pd.Series([2.5], dtype="float64"),
        "tpep_pickup_datetime": pd.to_datetime(["2021-01-01 10:00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2021-01-01 10:15:00"]),
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_validate_chunk_success():
    df = _sample_chunk()
    assert validate_chunk(df, chunk_count=0) is True


def test_validate_chunk_missing_required_column():
    df = _sample_chunk()
    df = df.drop(columns=["fare_amount"])
    with pytest.raises(ValueError):
        validate_chunk(df, chunk_count=1)


def test_validate_chunk_empty_returns_false():
    df = _sample_chunk().iloc[0:0]
    assert validate_chunk(df, chunk_count=2) is False


def test_validate_chunk_type_mismatch_warns_only():
    df = _sample_chunk(passenger_count=pd.Series(["two"], dtype="string"))
    # Type mismatches should not raise, only warn, so function should still return True
    assert validate_chunk(df, chunk_count=3) is True


def test_validate_chunk_value_ranges_warns_only():
    df = _sample_chunk(
        passenger_count=pd.Series([0], dtype="Int64"),  # invalid range
        trip_distance=pd.Series([-1.0], dtype="float64"),
        fare_amount=pd.Series([-5.0], dtype="float64"),
        tip_amount=pd.Series([-1.0], dtype="float64"),
        total_amount=pd.Series([-10.0], dtype="float64"),
        PULocationID=pd.Series([-1], dtype="Int64"),
        DOLocationID=pd.Series([-2], dtype="Int64"),
        tpep_dropoff_datetime=pd.to_datetime(["2020-12-31 09:00:00"]),
    )
    # Value range issues should log warnings but not raise
    assert validate_chunk(df, chunk_count=4) is True

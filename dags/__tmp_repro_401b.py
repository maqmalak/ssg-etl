"""Temporary repro #2: force the line-401 Int64 branch with many value shapes."""
import datetime as dt
from decimal import Decimal

import numpy as np
import pandas as pd


def infer_column_types(df):
    """Exact copy of infer_column_types() from the DAG."""
    for col in df.columns:
        series = df[col]
        if series.dtype == object:
            try:
                df[col] = pd.to_numeric(series, errors="ignore")
            except Exception:
                pass
        if series.dtype == object:
            non_null_series = series.dropna()
            if not non_null_series.empty:
                first_val = str(non_null_series.iloc[0]).lower()
                if any(p in first_val for p in ["/", "-", ":", "am", "pm", "gmt", "utc"]):
                    try:
                        df[col] = pd.to_datetime(series, errors="ignore", format="mixed")
                    except Exception:
                        try:
                            df[col] = pd.to_datetime(series, errors="ignore")
                        except Exception:
                            pass
    return df


def dag_chunk_convert(values, label, target_dtype="int64"):
    chunk = pd.DataFrame({"qty": values})
    dtypes = pd.Series({"qty": pd.api.types.pandas_dtype(target_dtype)})

    # Lines 386-405 of the DAG, verbatim
    chunk.columns = [c.lower() for c in chunk.columns]
    for col in chunk.columns:
        chunk[col] = chunk[col].replace("NaT", None)
    chunk = infer_column_types(chunk)
    after_infer = str(chunk["qty"].dtype)
    try:
        if dtypes["qty"] == "int64" or str(dtypes["qty"]) == "Int64":
            out = pd.to_numeric(chunk["qty"], errors="coerce").astype("Int64")
        else:
            out = pd.to_numeric(chunk["qty"], errors="coerce").astype("float64")
        print(f"OK    {label:34s} after_infer={after_infer:16s} -> {list(out)}")
    except Exception as e:  # noqa: BLE001
        print(f"FAIL  {label:34s} after_infer={after_infer:16s} -> {type(e).__name__}: {e}")


print("target dtype forced to int64 for all cases (column was int64 in first chunk)\n")
variants = [
    ("object int-strings", pd.Series(["10", "20"], dtype=object)),
    ("object frac-strings", pd.Series(["10", "10.5"], dtype=object)),
    ("object Decimal whole", pd.Series([Decimal("10"), Decimal("20")], dtype=object)),
    ("object Decimal frac", pd.Series([Decimal("10"), Decimal("10.5")], dtype=object)),
    ("object Decimal frac-only", pd.Series([Decimal("10.5")], dtype=object)),
    ("float64 non-integral", pd.Series([10.0, 10.5], dtype="float64")),
    ("float64 with inf", pd.Series([10.0, np.inf], dtype="float64")),
    ("float64 with nan", pd.Series([10.0, np.nan], dtype="float64")),
    ("object with None", pd.Series(["10", None], dtype=object)),
    ("object with junk", pd.Series(["10", "abc"], dtype=object)),
    ("object with NaT str", pd.Series(["10", "NaT"], dtype=object)),
    ("datetime64", pd.Series(pd.to_datetime(["2020-01-01"])).astype("datetime64[ns]")),
    ("object date-like str", pd.Series(["2020-01-01", "2020-02-01"], dtype=object)),
    ("huge int > int64", pd.Series([2 ** 70], dtype=object)),
    ("bool", pd.Series([True, False], dtype=bool)),
    ("complex", pd.Series([1 + 2j])),
    ("bytes", pd.Series([b"10", b"20"], dtype=object)),
    ("empty object", pd.Series([], dtype=object)),
    ("nested list value", pd.Series([[1, 2], [3, 4]], dtype=object)),
]

for label, values in variants:
    dag_chunk_convert(values, label)
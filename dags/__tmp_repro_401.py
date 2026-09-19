"""Temporary repro of the TypeError at line 401, mirroring the DAG's chunk loop."""
from decimal import Decimal

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


def dag_convert(chunk, dtypes, label):
    """Lines 386-405 of the DAG, verbatim."""
    chunk.columns = [c.lower() for c in chunk.columns]
    for col in chunk.columns:
        chunk[col] = chunk[col].replace("NaT", None)
    chunk = infer_column_types(chunk)
    for col in chunk.columns:
        if col in dtypes:
            try:
                if dtypes[col] == "int64" or str(dtypes[col]) == "Int64":
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("Int64")
                elif dtypes[col] == "float64":
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce").astype("float64")
            except Exception as e:
                print(f"{label}: {type(e).__name__}: {e}")
                print(f"   value types: {sorted({type(x).__name__ for x in chunk[col]})}")
                print(f"   values     : {[repr(x) for x in list(chunk[col])]}")
                return False
    print(f"{label}: OK -> {list(chunk[col])}")
    return True


# ---- Chunk 1: whole numeric values (MSSQL NUMERIC/DECIMAL -> Decimal) ----
first_chunk = pd.DataFrame({"qty": pd.Series([Decimal("2"), Decimal("3")], dtype=object)})
first_chunk = infer_column_types(first_chunk)
dtypes = first_chunk.dtypes.copy()
print("first chunk dtype for 'qty':", dict(dtypes))

# ---- Chunk 2: a fractional value appears in the same column ----
later = pd.DataFrame({"qty": pd.Series([Decimal("1.5"), Decimal("2.5")], dtype=object)})
dag_convert(later, dtypes, "chunk2 (Decimal 1.5)")

# ---- Other real-world variants of the same bug ----
dag_convert(pd.DataFrame({"qty": pd.Series([1.5, 2.5], dtype="float64")}), dtypes, "chunkX (float 1.5)")
dag_convert(pd.DataFrame({"qty": pd.Series([1.0, float("inf")], dtype="float64")}), dtypes, "chunkY (inf)")
dag_convert(pd.DataFrame({"qty": pd.Series([Decimal("4"), None], dtype=object)}), dtypes, "chunkZ (Decimal+None)")
"""Small, dependency-light checks for missing values and expected column formats."""

import pandas as pd


def check_dataframe(df, dataset, expected_columns, required_columns, max_missing_pct=1.0, types=None):
    """Validate one DataFrame and return a short list of plain result dictionaries.

    Blank strings are treated as missing values. Extra columns are allowed; missing
    expected columns fail the schema check. max_missing_pct applies to required columns.
    """
    results = []
    if df.empty:
        return [{"dataset": dataset, "check": "not_empty", "column": None,
                 "status": "FAIL", "message": "Dataset contains no rows."}]

    # Treat empty and whitespace-only strings as missing, just like NULL/NaN.
    clean = df.copy()
    for column in clean.select_dtypes(include=["object", "string"]).columns:
        clean[column] = clean[column].replace(r"^\s*$", pd.NA, regex=True)

    missing_columns = sorted(set(expected_columns) - set(clean.columns))
    results.append({
        "dataset": dataset, "check": "expected_columns", "column": None,
        "status": "FAIL" if missing_columns else "PASS",
        "message": (f"Missing columns: {', '.join(missing_columns)}" if missing_columns
                    else f"All {len(expected_columns)} expected columns are present."),
    })

    for column in required_columns:
        if column not in clean.columns:
            continue  # The missing-column check above already reports this.
        missing_pct = round(clean[column].isna().mean() * 100, 2)
        passed = missing_pct <= max_missing_pct
        results.append({
            "dataset": dataset, "check": "missing_values", "column": column,
            "status": "PASS" if passed else "FAIL",
            "message": f"{missing_pct}% missing; maximum allowed is {max_missing_pct}%.",
        })

    # Simple format checks for columns whose expected type is known.
    for column, expected_type in (types or {}).items():
        if column not in clean.columns:
            continue
        values = clean[column].dropna()
        if expected_type == "date":
            valid = pd.to_datetime(values, errors="coerce").notna().all()
        elif expected_type == "number":
            valid = pd.to_numeric(values, errors="coerce").notna().all()
        elif expected_type == "integer":
            converted = pd.to_numeric(values, errors="coerce")
            valid = converted.notna().all() and ((converted % 1) == 0).all()
        elif expected_type == "text":
            valid = values.map(lambda value: isinstance(value, str)).all()
        else:
            continue
        results.append({
            "dataset": dataset, "check": "expected_format", "column": column,
            "status": "PASS" if valid else "FAIL",
            "message": f"Values must be valid {expected_type} values.",
        })

    return results

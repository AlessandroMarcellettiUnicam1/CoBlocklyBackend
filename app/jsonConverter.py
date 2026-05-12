
from pathlib import Path
import pandas as pd
import json
from datetime import datetime

#----------------------------------dataset--------------------------------------

def load_dataset(data):
    # path = Path(path)

    # if not path.exists():
    #     raise FileNotFoundError(f"File not found: {path}")

    # if path.suffix.lower() == ".csv":
    #     return pd.read_csv(path)

    # if path.suffix.lower() == ".json":
    #     with open(path, "r", encoding="utf-8") as f:
    #         data = json.load(f)
        
    return pd.json_normalize(data)

    #raise ValueError("Unsupported file format. Use CSV or JSON.")

#-----------------------------------xesContent-------------------------------------

def normalize_timestamp(series):
    return pd.to_datetime(series, errors="coerce", utc=True)

def build_log_content(df, case_col, activity_col, timestamp_col):
    log = df.copy()

    # Normalize timestamp
    log[timestamp_col] = normalize_timestamp(log[timestamp_col])
    if log[timestamp_col].isna().any():
        raise ValueError("Some timestamps could not be parsed")

    # Add XES standard attributes (do NOT remove originals)
    log["case:concept:name"] = log[case_col]
    log["concept:name"] = log[activity_col]
    log["time:timestamp"] = log[timestamp_col]

    return log.sort_values(
        ["case:concept:name", "time:timestamp"]
    ).reset_index(drop=True)

#------------------------------------xesLog------------------------------------

def format_list_object(value):
    """
    Converts lists / dicts into a single string representation
    suitable for XES string attributes.
    """
    try:
        if isinstance(value, str):
            return value

        if isinstance(value, (list, dict)):
            return str(value)

        # Fallback
        return str(value)

    except Exception:
        return str(value)


# Attributes that must be serialized as a SINGLE string
# representing a list of objects
LIST_OBJECT_FIELDS = {k.lower() for k in {
    "events", "internalTxs", "inputs", "calls"
}}

def xes_attribute(key, value):

    # Special handling: list-of-objects as SINGLE string
    if key.lower() in LIST_OBJECT_FIELDS:
        serialized = format_list_object(value)
        escaped = (
            serialized
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        return f'<string key="{key}" value="{escaped}"/>'

    #if pd.isna(value):
    #    return None
    
    # Dates
    if isinstance(value, (pd.Timestamp, datetime)):
        return f'<date key="{key}" value="{value.isoformat()}"/>'

    # Integers
    if isinstance(value, int):
        return f'<int key="{key}" value="{value}"/>'

    # Floats
    if isinstance(value, float):
        return f'<float key="{key}" value="{value}"/>'

    # Booleans
    if isinstance(value, bool):
        return f'<boolean key="{key}" value="{str(value).lower()}"/>'

    # Default → string
    escaped = (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return f'<string key="{key}" value="{escaped}"/>'


def generate_xes(log_content, dataset_name):
    lines = []

    lines.append('<log xes.version="1.0" xes.features="nested-attributes">')
    lines.append(f'<string key="concept:name" value="{dataset_name}"/>')

    for case_id, case_df in log_content.groupby("case:concept:name"):
        lines.append("<trace>")
        lines.append(f'<string key="concept:name" value="{case_id}"/>')

        for _, row in case_df.iterrows():
            lines.append("<event>")

            for col, value in row.items():
                attr = xes_attribute(col, value)
                if attr is not None:
                    if isinstance(attr, list):
                        for a in attr:
                            if a is not None:
                                if not isinstance(a, str):
                                    print(f"A - WARNING: Non-string item: {type(a)} = {a}")
                                else:
                                    lines.append(a)
                    else:
                        if not isinstance(attr, str):
                            print(f"B - WARNING: Non-string item: {type(attr)} = {attr}")
                        else:
                            lines.append(attr)

            lines.append("</event>")

        lines.append("</trace>")

    lines.append("</log>")
    return "\n".join(lines)

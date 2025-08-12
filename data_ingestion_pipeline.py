import os
import re
import yaml
import pandas as pd
import logging
import gzip
from pathlib import Path

# Utility functions

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)
            return {}

def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string)
    return string

def col_header_val(df, table_config):
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\\w]', '_', regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x, '_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(), table_config['columns']))
    expected_col.sort()
    df.columns = list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col) == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file", mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded", missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0

def summarize_file(filepath, sep, compression=None):
    if compression == "gzip":
        with gzip.open(filepath, "rt", encoding="utf-8", errors="ignore") as f:
            header = f.readline()
            cols = len(header.strip().split(sep))
            rows = sum(1 for _ in f)
    else:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            header = f.readline()
            cols = len(header.strip().split(sep))
            rows = sum(1 for _ in f)
    size = os.path.getsize(filepath)
    print(f"Summary:\n- Total rows: {rows}\n- Total columns: {cols}\n- File size: {size/1024**2:.2f} MB")
    return {"rows": rows, "columns": cols, "file_size_bytes": size}

# Ingestion functions

def pandas_ingest(config):
    print("Reading with pandas...")
    df = pd.read_csv(config['file_name'], delimiter=config['inbound_delimiter'], engine="python", on_bad_lines="skip")
    col_header_val(df, config)
    return df

def write_pipe_gz(df, config):
    out_path = Path(config['file_name']).with_suffix('.pipe.gz')
    df.to_csv(out_path, sep=config['outbound_delimiter'], index=False, header=True, compression="gzip")
    print(f"Written gzipped pipe-separated file: {out_path}")
    return str(out_path)

# Main pipeline

def main():
    # Load config
    config_path = "file.yaml"
    config = read_config_file(config_path)
    print("Config loaded:", config)

    # Try pandas
    df_pandas = pandas_ingest(config)
    summarize_file(config['file_name'], config['inbound_delimiter'])

    # Write gzipped pipe-separated file
    out_file = write_pipe_gz(df_pandas, config)
    summarize_file(out_file, config['outbound_delimiter'], compression="gzip")

if __name__ == "__main__":
    main()
import os
import sys
import time
import json
import re
import psutil
import yaml
import importlib.util
from pathlib import Path
import csv
csv.field_size_limit(10**7)

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "config.yaml"

def sanitize_name(name: str) -> str:
    n = str(name).strip().lower()
    n = re.sub(r"[^\w]+", "_", n)
    n = re.sub(r"_+", "_", n)
    return n.strip("_")

def sanitize_columns(cols):
    return [sanitize_name(c) for c in cols]

def format_bytes(n):
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if n < 1024.0:
            return f"{n:3.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"

def measure(func, *args, **kwargs):
    proc = psutil.Process(os.getpid())
    mem_before = proc.memory_info().rss
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    mem_after = proc.memory_info().rss
    return result, {"seconds": elapsed, "rss_delta": mem_after - mem_before}

def read_yaml_config(p: Path):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def write_yaml_config(p: Path, cfg: dict):
    with open(p, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)

def validate_schema(actual_cols, expected_cols):
    a = list(actual_cols)
    e = list(expected_cols)
    return (a == e, {"actual_count": len(a), "expected_count": len(e), "actual": a, "expected": e})

def ensure_config():
    cfg = read_yaml_config(CONFIG_PATH)
    # Defaults for robust CSV parsing
    cfg.setdefault("read", {})
    cfg["read"].setdefault("encoding", "utf-8")
    cfg["read"].setdefault("sep", ",")
    cfg["read"].setdefault("on_bad_lines", "skip")       # skip malformed rows
    cfg["read"].setdefault("quotechar", '"')             # default quote
    cfg["read"].setdefault("doublequote", True)          # "" inside quotes
    cfg["read"].setdefault("escapechar", "\\")           # escape char
    cfg.setdefault("write", {})
    cfg["write"].setdefault("compression", "gzip")

    src = Path(cfg["read"]["path"])
    sep = cfg["read"]["sep"]
    if not src.exists():
        raise FileNotFoundError(f"Input file not found: {src}")
    # Infer schema if not provided
    if not cfg.get("schema") or not cfg["schema"].get("columns"):
        with open(src, "r", encoding=cfg["read"].get("encoding", "utf-8"), errors="ignore") as f:
            header = f.readline().rstrip("\n")
        raw_cols = header.split(sep)
        san_cols = sanitize_columns(raw_cols)
        cfg.setdefault("schema", {})["columns"] = san_cols
        write_yaml_config(CONFIG_PATH, cfg)
        print(f"Inferred schema with {len(san_cols)} columns and wrote back to config.yaml")
    return cfg

def try_import(mod):
    return importlib.util.find_spec(mod) is not None

def filter_bad_lines(input_path, output_path, sep=","):
    with open(input_path, "r", encoding="utf-8", errors="ignore") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:
        header = fin.readline()
        fout.write(header)
        expected_cols = len(header.rstrip("\n").split(sep))
        for line in fin:
            # Only keep lines with the correct number of separators
            if line.count(sep) == expected_cols - 1:
                fout.write(line)

def bench_pandas(path, sep, nrows, expected_cols):
    import pandas as pd
    # Pre-filter bad lines to a temp file
    filtered_path = str(Path(path).with_suffix(".filtered.csv"))
    if not os.path.exists(filtered_path):
        print("Filtering bad lines for pandas sample...")
        filter_bad_lines(path, filtered_path)
    read_kwargs = dict(
        sep=sep,
        nrows=nrows,
        dtype_backend="pyarrow",
        encoding="utf-8",
        engine="python",
        on_bad_lines="skip",
        quotechar='"',
        doublequote=True,
        escapechar="\\",
    )
    df = pd.read_csv(filtered_path, **read_kwargs)
    df.columns = sanitize_columns(df.columns)
    ok, _ = validate_schema(df.columns.tolist(), expected_cols[:len(df.columns)])
    return {"rows": len(df), "cols": df.shape[1], "schema_ok": ok}

def bench_dask(path, sep, nrows, expected_cols):
    import dask.dataframe as dd
    df = dd.read_csv(
        path,
        sep=sep,
        dtype_backend="pyarrow",
        blocksize="256MB",
        assume_missing=True,
        on_bad_lines="skip",
        quotechar='"',
        doublequote=True,
        escapechar="\\",
        encoding="utf-8",
    )
    if nrows:
        pdf = df.head(nrows, compute=True)
        pdf.columns = sanitize_columns(pdf.columns)
        ok, _ = validate_schema(pdf.columns.tolist(), expected_cols[:len(pdf.columns)])
        return {"rows": len(pdf), "cols": pdf.shape[1], "schema_ok": ok}
    cols = sanitize_columns(list(df.columns))
    ok, _ = validate_schema(cols, expected_cols[:len(cols)])
    return {"rows": None, "cols": len(cols), "schema_ok": ok}

def bench_modin(path, sep, nrows, expected_cols):
    import os as _os
    _os.environ["MODIN_ENGINE"] = "ray"
    import ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, include_dashboard=False)
    import modin.pandas as mpd
    # Pre-filter bad lines to a temp file
    filtered_path = str(Path(path).with_suffix(".filtered.csv"))
    if not os.path.exists(filtered_path):
        print("Filtering bad lines for modin sample...")
        filter_bad_lines(path, filtered_path)
    df = mpd.read_csv(filtered_path, sep=sep, nrows=nrows)
    df.columns = sanitize_columns(list(df.columns))
    ok, _ = validate_schema(list(df.columns), expected_cols[:len(df.columns)])
    _ = int(df.shape[0])  # materialize
    return {"rows": int(df.shape[0]), "cols": int(df.shape[1]), "schema_ok": ok}

def bench_raydata(path, sep, nrows, expected_cols):
    import ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, include_dashboard=False)
    ds = ray.data.read_csv(path, read_options={"delimiter": sep})
    if nrows:
        ds = ds.limit(nrows)
    cnt = ds.count()
    cols = sanitize_columns(ds.schema().names())
    ok, _ = validate_schema(cols, expected_cols[:len(cols)])
    return {"rows": cnt, "cols": len(cols), "schema_ok": ok}

def run_benchmarks(cfg):
    path = cfg["read"]["path"]
    sep = cfg["read"]["sep"]
    n = cfg["benchmark"]["sample_rows"]
    expected_cols = cfg["schema"]["columns"]
    results = {}

    res, m = measure(bench_pandas, path, sep, n, expected_cols)
    results["pandas"] = {"result": res, "seconds": round(m["seconds"], 3), "rss_delta": m["rss_delta"]}

    if try_import("dask"):
        res, m = measure(bench_dask, path, sep, n, expected_cols)
        results["dask"] = {"result": res, "seconds": round(m["seconds"], 3), "rss_delta": m["rss_delta"]}

    if try_import("modin") and try_import("ray"):
        try:
            res, m = measure(bench_modin, path, sep, n, expected_cols)
            results["modin(ray)"] = {"result": res, "seconds": round(m["seconds"], 3), "rss_delta": m["rss_delta"]}
        except Exception as e:
            results["modin(ray)"] = {"error": str(e)}

    if try_import("ray"):
        try:
            res, m = measure(bench_raydata, path, sep, n, expected_cols)
            results["ray.data"] = {"result": res, "seconds": round(m["seconds"], 3), "rss_delta": m["rss_delta"]}
        except Exception as e:
            results["ray.data"] = {"error": str(e)}

    out = HERE / "results_benchmark.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"Wrote benchmark: {out}")
    return results

def dask_ingest_full(cfg):
    import dask.dataframe as dd
    path = cfg["read"]["path"]
    in_sep = cfg["read"]["sep"]
    out_path = cfg["write"]["path"]
    out_sep = cfg["write"]["sep"]
    expected_cols = cfg["schema"]["columns"]

    df = dd.read_csv(
        path,
        sep=in_sep,
        dtype_backend="pyarrow",
        blocksize="256MB",
        assume_missing=True,
        on_bad_lines="skip",
        quotechar='"',
        doublequote=True,
        escapechar="\\",
        encoding="utf-8",
    )
    new_cols = sanitize_columns(list(df.columns))
    df = df.rename(columns=dict(zip(df.columns, new_cols)))
    present = [c for c in expected_cols if c in df.columns]
    df = df[present]

    ok, meta = validate_schema(list(df.columns), expected_cols)
    print(f"Schema validation: {ok}")
    if not ok:
        print(json.dumps(meta, indent=2))

    t0 = time.perf_counter()
    df.to_csv(out_path, sep=out_sep, index=False, header=True, compression="gzip", single_file=True)
    elapsed = time.perf_counter() - t0
    rows = int(df.shape[0].compute())
    return rows, elapsed

def pandas_ingest_full_chunked(cfg, chunksize=1_000_000):
    import pandas as pd
    path = cfg["read"]["path"]
    in_sep = cfg["read"]["sep"]
    out_path = cfg["write"]["path"]
    out_sep = cfg["write"]["sep"]
    expected_cols = cfg["schema"]["columns"]

    # Pre-filter bad lines to a temp file
    filtered_path = str(Path(path).with_suffix(".filtered.csv"))
    if not os.path.exists(filtered_path):
        print("Filtering bad lines for pandas sample...")
        filter_bad_lines(path, filtered_path, ",")

    try:
        Path(out_path).unlink()
    except FileNotFoundError:
        pass

    first = True
    total_rows = 0
    t0 = time.perf_counter()
    with open(filtered_path, "r", encoding="utf-8") as fin, \
         open(out_path, "wb") as gzout:
        import gzip
        gz = gzip.open(gzout, "wt", newline="", encoding="utf-8")
        for chunk in pd.read_csv(
            fin,
            sep=in_sep,
            chunksize=chunksize,
            dtype_backend="pyarrow",
            engine="python",
            on_bad_lines="skip",
            quotechar='"',
            doublequote=True,
            escapechar="\\",
            encoding="utf-8",
        ):
            chunk.columns = sanitize_columns(chunk.columns)
            chunk = chunk[[c for c in expected_cols if c in chunk.columns]]
            if first:
                ok, meta = validate_schema(list(chunk.columns), expected_cols)
                print(f"Schema validation: {ok}")
                if not ok:
                    print(json.dumps(meta, indent=2))
            chunk.to_csv(gz, sep=out_sep, index=False, header=first)
            total_rows += len(chunk)
            first = False
        gz.close()
    elapsed = time.perf_counter() - t0
    return total_rows, elapsed

def summarize_output(cfg):
    out_path = cfg["write"]["path"]
    sep = cfg["write"]["sep"]
    try:
        import dask.dataframe as dd
        df = dd.read_csv(out_path, sep=sep, compression="gzip", dtype_backend="pyarrow")
        rows = int(df.shape[0].compute())
        cols = len(df.columns)
    except Exception:
        # Fallback: quick header + count by streaming (slower for huge files)
        import gzip
        with gzip.open(out_path, "rt", encoding="utf-8", errors="ignore") as f:
            cols = len(f.readline().split(sep))
            rows = sum(1 for _ in f)
    size = Path(out_path).stat().st_size
    summary = {
        "rows": rows,
        "columns": cols,
        "file_size_bytes": size,
        "file_size_human": format_bytes(size),
        "output": str(out_path)
    }
    with open(HERE / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print("Summary:")
    print(f"- Total rows: {rows}")
    print(f"- Total columns: {cols}")
    print(f"- File size: {format_bytes(size)}")
    print(f"Wrote summary: {HERE / 'summary.json'}")
    return summary

def main():
    cfg = ensure_config()
    src = Path(cfg["read"]["path"])
    print(f"Input: {src} ({format_bytes(src.stat().st_size)})")
    print("Benchmarking read engines on a sample...")
    results = run_benchmarks(cfg)
    print(json.dumps(results, indent=2, default=str))

    print("Ingesting full file to gzipped pipe-separated output...")
    rows, elapsed = None, None
    if try_import("dask"):
        try:
            rows, elapsed = dask_ingest_full(cfg)
            print(f"Dask write completed in {elapsed:.2f}s; rows={rows}")
        except Exception as e:
            print(f"Dask path failed: {e}. Falling back to pandas chunked...")
    if rows is None:
        rows, elapsed = pandas_ingest_full_chunked(cfg)
        print(f"Pandas chunked write completed in {elapsed:.2f}s; rows={rows}")

    summarize_output(cfg)

if __name__ == "__main__":
    sys.exit(main())
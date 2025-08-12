# Data Ingestion Pipeline

This repository provides a robust data ingestion pipeline for large CSV/text files (2+ GB), following best practices for data validation, schema management, and efficient file writing. The pipeline is designed for use in Python environments and is inspired by the requirements and workflow demonstrated in the provided Jupyter notebook.

## Features

- **Configurable ingestion via YAML file**
- **Column name sanitization and validation**
- **Efficient reading of large files using pandas**
- **Pipe-separated output in gzipped format**
- **Automatic summary generation (row count, column count, file size)**
- **Easy to extend for other file formats or validation rules**

## Folder Structure

```
e:\Data ingestion\
│
├── data_ingestion_pipeline.py   # Main ingestion script
├── file.yaml                   # Configuration file (edit for your schema)
├── linkedin_job.csv            # Example large CSV file (replace with your own)
├── linkedin_job.pipe.gz        # Output gzipped, pipe-separated file (generated)
├── README.md                   # This documentation
└── ...                         # Any other supporting files
```

## Usage

1. **Edit `file.yaml`**  
   Specify your file name, delimiters, and column names.  
   Example:
   ```yaml
   file_type: csv
   dataset_name: linkedin_job
   file_name: linkedin_job.csv
   table_name: linkedin_jobs_table
   inbound_delimiter: ","
   outbound_delimiter: "|"
   skip_leading_rows: 1
   columns:
     - job_link
     - last_processed_time
     - got_summary
     - got_ner
     - is_being_worked
     - job_title
     - company
     - job_location
     - first_seen
     - search_city
     - search_country
     - search_position
     - job_level
     - job_type
     - job_skills
     - job_summary
   ```

2. **Run the pipeline**
   ```bash
   python data_ingestion_pipeline.py
   ```

3. **Output**
   - The script will read your CSV, validate columns, write a gzipped pipe-separated file, and print a summary.

## Example Summary Output

```
Config loaded: {...}
Reading with pandas...
column name and column length validation passed
Summary:
- Total rows: 48270914
- Total columns: 16
- File size: 5623.03 MB
Written gzipped pipe-separated file: linkedin_job.pipe.gz
Summary:
- Total rows: 48270914
- Total columns: 16
- File size: 1234.56 MB
```

## Requirements

- Python 3.8+
- pandas
- pyyaml

Install dependencies:
```bash
pip install pandas pyyaml
```

## Notes

- For very large files, ensure you have sufficient disk space and memory.
- The pipeline skips malformed rows and sanitizes column names for consistency.
- You can adapt the YAML config and script for other schemas or delimiters.

---

**Author:**  
Keval Savaliya  

#!/usr/bin/env python3
"""Test script to verify V2 Parquet files can be read by pyarrow"""
import sys
import pyarrow.parquet as pq

if len(sys.argv) < 2:
    print("Usage: python3 test-v2-pyarrow.py <parquet-file>")
    sys.exit(1)

parquet_file = sys.argv[1]

try:
    table = pq.read_table(parquet_file)
    print(f"✅ pyarrow can read V2 file")
    print(f"Rows: {len(table)}")
    print(f"Columns: {table.column_names}")
    if len(table) > 0:
        print(f"First row id: {table['id'][0].as_py()}")
        if 'name' in table.column_names:
            print(f"First row name: {table['name'][0].as_py()}")
        if 'score' in table.column_names:
            print(f"First row score: {table['score'][0].as_py()}")
    sys.exit(0)
except Exception as e:
    print(f"❌ pyarrow error: {e}")
    sys.exit(1)



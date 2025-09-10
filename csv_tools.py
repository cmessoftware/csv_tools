#!/usr/bin/env python3

import sys
import polars as pl
from pathlib import Path

def help():
    print("Available commands:")
    print("  clean <input> <output>")
    print("  filter <input> <output> <column> <value>")
    print("  check <input>")
    print("  count <input>")
    print("  count_all <file_list>")
    print("  merge_dedup <file_list> <output>")

def clean_headers(input_file, output_file):
    df = pl.read_csv(input_file)
    df.write_csv(output_file)
    print(f"Headers cleaned and written to {output_file}.")

def filter_rows(input_file, output_file, column, value):
    df = pl.read_csv(input_file)
    df_filtered = df.filter(pl.col(column) == value)
    df_filtered.write_csv(output_file)
    print(f"Filtered rows saved to {output_file}.")

def check_duplicate_header(input_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    if len(lines) < 2:
        print("No duplicate header found (file too short).")
        return
    header = lines[0].strip()
    for idx, line in enumerate(lines[1:], start=2):
        if line.strip() == header:
            print(f"Duplicate header found at line {idx}")
            return
    print("No duplicate header found.")

def count_lines(input_file):
    df = pl.read_csv(input_file)
    print(f"{input_file}: {df.height} rows.")

def count_all(file_list_path):
    total = 0
    with open(file_list_path, 'r', encoding='utf-8') as f:
        files = [line.strip() for line in f if line.strip()]
    for file in files:
        df = pl.read_csv(file)
        print(f"{file}: {df.height} rows.")
        total += df.height
    print(f"Total rows in all files: {total}")

def merge_dedup(file_list_path, output_file):
    with open(file_list_path, 'r', encoding='utf-8') as f:
        files = [line.strip() for line in f if line.strip()]
    dfs = [pl.read_csv(file) for file in files]
    merged = pl.concat(dfs).unique()
    merged.write_csv(output_file)
    print(f"Merged and deduplicated file written to {output_file}.")

def main():
    args = sys.argv
    if len(args) < 2:
        help()
        return

    cmd = args[1]
    if cmd == "clean" and len(args) == 4:
        clean_headers(args[2], args[3])
    elif cmd == "filter" and len(args) == 6:
        filter_rows(args[2], args[3], args[4], args[5])
    elif cmd == "check" and len(args) == 3:
        check_duplicate_header(args[2])
    elif cmd == "count" and len(args) == 3:
        count_lines(args[2])
    elif cmd == "count_all" and len(args) == 3:
        count_all(args[2])
    elif cmd == "merge_dedup" and len(args) == 4:
        merge_dedup(args[2], args[3])
    else:
        help()

if __name__ == "__main__":
    main()

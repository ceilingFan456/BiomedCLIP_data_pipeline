#!/usr/bin/env python3
import argparse
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument("--out_dir", required=True, type=Path)
    ap.add_argument("--prefix", default="pubmed_open_access_file_list")
    ap.add_argument("--shard_size", type=int, default=10000)  # rows per shard (excluding header)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    with args.input.open("r", encoding="utf-8") as fin:
        header = fin.readline()
        shard_idx = 0
        rows_in_shard = 0
        fout = None
        for line in fin:
            if rows_in_shard == 0:
                if fout:
                    fout.close()
                shard_path = args.out_dir / f"{args.prefix}_shard-{shard_idx:05d}.txt"
                fout = shard_path.open("w", encoding="utf-8")
                fout.write(header)
                shard_idx += 1
            fout.write(line)
            rows_in_shard += 1
            if rows_in_shard >= args.shard_size:
                rows_in_shard = 0
        if fout:
            fout.close()
    print("Done.")

if __name__ == "__main__":
    main()

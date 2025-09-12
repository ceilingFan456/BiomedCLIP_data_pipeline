# split_pubmed_list.py
import hashlib
from pathlib import Path

src = Path("/home/azureuser/BiomedCLIP_data_pipeline/_results/data/pubmed_open_access_file_list.txt")
dst_dir = src.parent

train = dst_dir / "pubmed_open_access_file_list_train.txt"
dev   = dst_dir / "pubmed_open_access_file_list_dev.txt"
test  = dst_dir / "pubmed_open_access_file_list_test.txt"

with src.open("r", encoding="utf-8") as fin, \
     train.open("w", encoding="utf-8") as ftr, \
     dev.open("w", encoding="utf-8") as fdv, \
     test.open("w", encoding="utf-8") as fte:

    header = next(fin)  # first line is header
    for f in (ftr, fdv, fte):
        f.write(header)

    for line in fin:
        # expected columns: path, title, pmcid, pmid, code   (tab-separated)
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            continue
        pmcid = parts[2]

        # stable "random" bucket from hash of PMCID
        h = int(hashlib.md5(pmcid.encode("utf-8")).hexdigest()[:8], 16) % 100

        # 95% train, 2% dev, 3% test (adjust if you prefer 95/2.5/2.5)
        if h < 97:
            ftr.write(line)
        elif h < 98:
            fdv.write(line)
        else:
            fte.write(line)

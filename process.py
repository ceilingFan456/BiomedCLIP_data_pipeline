from __future__ import annotations

import io
import json
import tarfile
import tempfile
from pathlib import Path
from typing import Optional, List, Dict
from PIL import Image
from tqdm import tqdm
import requests
from lxml import etree
import pubmed_parser

# --- Config ---
# Make sure repo_root is a Path, not str
repo_root = Path("/home/azureuser/disk")  # EDIT as needed

PUBMED_OPEN_ACCESS_BASE_URL = "https://pmc-oa-opendata.s3.amazonaws.com/"
# "https://ftp.ncbi.nlm.nih.gov/pub/pmc/"
# Example: "oa_package/00/00/PMC15015.tar.gz" (depends on your list's "path" column)

# --- Helpers ---
def _stream_download(url: str, dst_path: Path, chunk_size: int = 1024 * 1024) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)

def _response_size(url: str) -> Optional[int]:
    try:
        r = requests.head(url, timeout=30, allow_redirects=True)
        if "Content-Length" in r.headers:
            return int(r.headers["Content-Length"])
    except Exception:
        pass
    return None

def _safe_open_tar(path: Path) -> tarfile.TarFile:
    # Use r:gz; if some are plain .tar, change to auto mode "r:*"
    return tarfile.open(path, mode="r:gz")

def _members_by_ext(tar: tarfile.TarFile, exts: List[str]) -> List[tarfile.TarInfo]:
    L = []
    lowexts = tuple(e.lower() for e in exts)
    for m in tar.getmembers():
        name = m.name.lower()
        if name.endswith(lowexts):
            L.append(m)
    return L

def _extract_member_to_bytes(tar: tarfile.TarFile, member: tarfile.TarInfo) -> bytes:
    f = tar.extractfile(member)
    return f.read() if f else b""

def _ensure_jpeg_and_save(img_bytes: bytes, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(io.BytesIO(img_bytes)) as im:
        # Convert to RGB JPEG
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        elif im.mode == "L":
            im = im.convert("RGB")
        # Standardize to .jpg
        if out_path.suffix.lower() != ".jpg":
            out_path = out_path.with_suffix(".jpg")
        im.save(out_path, format="JPEG", quality=90, optimize=True)
    return out_path

def _load_pubmed_figs_from_nxml_bytes(nxml_bytes: bytes) -> List[Dict]:
    """Return list of dicts from pubmed_parser.parse_pubmed_caption format."""
    # pubmed_parser expects a file path; we can parse here with lxml and replicate,
    # but to keep your existing behavior, we use pubmed_parser via a temp file.
    with tempfile.NamedTemporaryFile(suffix=".nxml", delete=False) as tf:
        tf.write(nxml_bytes)
        tmp_path = tf.name
    try:
        return pubmed_parser.parse_pubmed_caption(tmp_path)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass

# --- Main streaming builder ---
def build_pubmed_image_caption_dataset_streaming(
    file_list_path: Path = repo_root / "_results/data/pubmed_open_access_file_list.txt",
    jsonl_out_path: Path = repo_root / "_results/data/pmc15_cc12m_like.jsonl",
    images_out_dir: Path = repo_root / "_results/data/pmc15_cc12m_like_images",
    subset_size: Optional[int] = None,
    file_extension: str = ".tar.gz",
    max_filesize_mb: Optional[int] = None,
    resume: bool = True,
):
    """
    Streams PMC OA packages one-by-one, extracts only needed files, writes CC12M-like JSONL, and deletes temp files.

    JSONL schema per line:
      {
        "image": "<relative path under images_out_dir>",
        "caption": "<figure caption>",
        "pmid": "<pmid>",
        "pmcid": "<pmcid>",
        "pair_id": "<pmid>_<fig_id>"
      }
    """
    images_out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_out_path.parent.mkdir(parents=True, exist_ok=True)

    # Resume support: collect existing pairs to skip duplicates
    existing_pairs = set()
    if resume and jsonl_out_path.exists():
        with jsonl_out_path.open("r") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    existing_pairs.add(obj.get("pair_id", ""))
                except Exception:
                    continue

    # Read list
    entries: List[Dict[str, str]] = []
    with open(file_list_path, "r") as file:
        lines = file.readlines()[1:]  # skip header
        for idx, line in enumerate(lines):
            if subset_size and idx + 1 > subset_size:
                break
            try:
                path, title, pmcid, pmid, code = line.rstrip("\n").split("\t")
                entries.append({"path": path, "title": title, "pmcid": pmcid, "pmid": pmid, "code": code})
            except ValueError:
                continue

    pbar = tqdm(entries, desc="Processing PMC OA packages")
    n_written = 0
    n_skipped = 0

    with jsonl_out_path.open("a", buffering=1) as jf:
        for ent in pbar:
            pmcid = ent["pmcid"]
            pmid = ent["pmid"]
            url = PUBMED_OPEN_ACCESS_BASE_URL + ent["path"]
            # Enforce extension if needed (your list might already include .tar.gz)
            if not url.endswith(file_extension):
                url = url + file_extension

            # Optional size guard
            size_bytes = _response_size(url)
            if max_filesize_mb is not None and size_bytes is not None:
                if size_bytes > max_filesize_mb * 1024 * 1024:
                    tqdm.write(f"[Skip] {pmcid} > {max_filesize_mb} MB ({size_bytes/1024/1024:.1f} MB)")
                    n_skipped += 1
                    continue

            # Download to temp file
            try:
                with tempfile.TemporaryDirectory() as td:
                    tar_path = Path(td) / f"{pmcid}{file_extension}"
                    _stream_download(url, tar_path)

                    # Open tar and pick nxml + images
                    with _safe_open_tar(tar_path) as tar:
                        nxml_members = _members_by_ext(tar, [".nxml"])
                        if not nxml_members:
                            tqdm.write(f"[Warn] No NXML in {pmcid}, skipping.")
                            n_skipped += 1
                            continue

                        # Assume one main NXML (if multiple, process first)
                        nxml_bytes = _extract_member_to_bytes(tar, nxml_members[0])
                        try:
                            parsed_figs = _load_pubmed_figs_from_nxml_bytes(nxml_bytes)
                        except (etree.XMLSyntaxError, AttributeError) as e:
                            tqdm.write(f"[Warn] NXML parse failed for {pmcid}: {e}")
                            n_skipped += 1
                            continue

                        if not parsed_figs:
                            n_skipped += 1
                            continue

                        # Build a name -> TarInfo dict for quick image lookup
                        # Normalize by basename so we can match <graphic_ref> reliably.
                        img_tar_members = {
                            Path(m.name).name.lower(): m
                            for m in tar.getmembers()
                            if m.isfile() and any(m.name.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".tif", ".tiff"))
                        }

                        # pmid/pmc are repeated in parsed_figs; take from first
                        _pmid = parsed_figs[0].get("pmid", pmid) or pmid
                        _pmc = parsed_figs[0].get("pmc", pmcid) or pmcid

                        wrote_any = False
                        for fig in parsed_figs:
                            fig_id = str(fig.get("fig_id", ""))
                            fig_caption = str(fig.get("fig_caption", "")).strip()
                            graphic_ref = fig.get("graphic_ref", "")
                            if not fig_caption or not graphic_ref:
                                continue

                            # The parser's graphic_ref is typically without extension; try common extensions
                            candidates = []
                            base = Path(graphic_ref).name
                            stem = Path(base).stem
                            # Try exact name if present (sometimes graphic_ref already has ext)
                            candidates.append(base.lower())
                            # Try with common suffixes
                            for ext in (".jpg", ".jpeg", ".png", ".tif", ".tiff"):
                                candidates.append((stem + ext).lower())

                            member = None
                            for c in candidates:
                                if c in img_tar_members:
                                    member = img_tar_members[c]
                                    break
                            if member is None:
                                # Some packages place figures under subfolders with altered names; try looser search
                                for m in img_tar_members:
                                    if stem in m:
                                        member = img_tar_members[m]
                                        break

                            if member is None:
                                # No image found for this figure
                                continue

                            pair_id = f"{_pmid}_{fig_id}"
                            if resume and pair_id in existing_pairs:
                                # Image was already processed/output
                                continue

                            img_bytes = _extract_member_to_bytes(tar, member)
                            # Standard target path: images_out_dir/PMCxxxx/PMCxxxx_fig-<fig_id>.jpg
                            img_rel = Path(_pmc) / f"{_pmc}_fig-{fig_id}.jpg"
                            img_abs = images_out_dir / img_rel

                            try:
                                saved_path = _ensure_jpeg_and_save(img_bytes, img_abs)
                            except Exception as e:
                                tqdm.write(f"[Warn] Failed to write image for {pair_id}: {e}")
                                continue

                            record = {
                                "image": str(saved_path.relative_to(images_out_dir).as_posix()),
                                "caption": fig_caption,
                                "pmid": _pmid,
                                "pmcid": _pmc,
                                "pair_id": pair_id,
                            }
                            jf.write(json.dumps(record, ensure_ascii=False) + "\n")
                            existing_pairs.add(pair_id)
                            n_written += 1
                            wrote_any = True

                        if not wrote_any:
                            n_skipped += 1

            except requests.HTTPError as e:
                tqdm.write(f"[HTTP] {pmcid} failed: {e}")
                n_skipped += 1
                continue
            except tarfile.ReadError as e:
                tqdm.write(f"[TAR] {pmcid} bad archive: {e}")
                n_skipped += 1
                continue
            except Exception as e:
                tqdm.write(f"[Error] {pmcid}: {e}")
                n_skipped += 1
                continue

            pbar.set_postfix(written=n_written, skipped=n_skipped)

    print(f"Done. Wrote {n_written} pairs. Skipped {n_skipped} items.")



# =======================
# Splits & Shards Orchestration
# =======================
from dataclasses import dataclass

# Optional dual-base fallback: try S3 then NCBI FTP
PRIMARY_BASE = "https://pmc-oa-opendata.s3.amazonaws.com/"
FALLBACK_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/"

@dataclass
class SplitSpec:
    name: str                # e.g., "dev", "test", "train"
    list_path: Path          # TSV list file for this split (or shard)
    jsonl_name: str          # output JSONL file name
    images_subdir: Path      # subdir under images root where images will go

def _try_download_with_fallback(rel_path: str, dst: Path, file_extension: str = ".tar.gz"):
    # Ensure extension on the relative path if needed
    rel = rel_path if rel_path.endswith(file_extension) else (rel_path + file_extension)

    # First try S3
    s3_url = PRIMARY_BASE + rel
    try:
        _stream_download(s3_url, dst)
        return
    except Exception as e1:
        # Then try NCBI FTP
        ftp_url = FALLBACK_BASE + rel
        try:
            _stream_download(ftp_url, dst)
            return
        except Exception as e2:
            raise RuntimeError(f"Failed S3 ({e1}) and FTP ({e2}) for {rel_path}")

def build_pubmed_image_caption_dataset_streaming(
    file_list_path: Path,
    jsonl_out_path: Path,
    images_out_dir: Path,
    subset_size: Optional[int] = None,
    file_extension: str = ".tar.gz",
    max_filesize_mb: Optional[int] = None,
    resume: bool = True,
    use_dual_base: bool = True,
):
    """
    Streams PMC OA packages one-by-one, extracts only needed files, writes CC12M-like JSONL, and deletes temp files.
    (Same schema as your version; now supports dual-base fallback.)
    """
    images_out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_out_path.parent.mkdir(parents=True, exist_ok=True)

    existing_pairs = set()
    if resume and jsonl_out_path.exists():
        with jsonl_out_path.open("r") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    pid = obj.get("pair_id", "")
                    if pid:
                        existing_pairs.add(pid)
                except Exception:
                    continue

    # Read list
    entries: List[Dict[str, str]] = []
    with open(file_list_path, "r") as file:
        lines = file.readlines()[1:]  # skip header
        for idx, line in enumerate(lines):
            if subset_size and idx + 1 > subset_size:
                break
            try:
                path, title, pmcid, pmid, code = line.rstrip("\n").split("\t")
                entries.append({"path": path, "title": title, "pmcid": pmcid, "pmid": pmid, "code": code})
            except ValueError:
                continue

    pbar = tqdm(entries, desc=f"Processing {file_list_path.name}")
    n_written = 0
    n_skipped = 0

    with jsonl_out_path.open("a", buffering=1) as jf:
        for ent in pbar:
            pmcid = ent["pmcid"]
            pmid = ent["pmid"]
            rel_path = ent["path"]

            # Optional size check (S3 only; HEAD may be blocked on FTP)
            if max_filesize_mb is not None:
                size_bytes = _response_size(PRIMARY_BASE + (rel_path if rel_path.endswith(file_extension) else rel_path + file_extension))
                if size_bytes is not None and size_bytes > max_filesize_mb * 1024 * 1024:
                    tqdm.write(f"[Skip] {pmcid} > {max_filesize_mb} MB ({size_bytes/1024/1024:.1f} MB)")
                    n_skipped += 1
                    continue

            try:
                with tempfile.TemporaryDirectory() as td:
                    tar_path = Path(td) / f"{pmcid}{file_extension}"
                    # download (with optional fallback)
                    if use_dual_base:
                        _try_download_with_fallback(rel_path, tar_path, file_extension=file_extension)
                    else:
                        url = PRIMARY_BASE + (rel_path if rel_path.endswith(file_extension) else rel_path + file_extension)
                        _stream_download(url, tar_path)

                    with _safe_open_tar(tar_path) as tar:
                        nxml_members = _members_by_ext(tar, [".nxml"])
                        if not nxml_members:
                            n_skipped += 1
                            continue

                        nxml_bytes = _extract_member_to_bytes(tar, nxml_members[0])
                        try:
                            parsed_figs = _load_pubmed_figs_from_nxml_bytes(nxml_bytes)
                        except (etree.XMLSyntaxError, AttributeError):
                            n_skipped += 1
                            continue
                        if not parsed_figs:
                            n_skipped += 1
                            continue

                        img_tar_members = {
                            Path(m.name).name.lower(): m
                            for m in tar.getmembers()
                            if m.isfile() and any(m.name.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".tif", ".tiff"))
                        }

                        _pmid = parsed_figs[0].get("pmid", pmid) or pmid
                        _pmc  = parsed_figs[0].get("pmc", pmcid) or pmcid

                        wrote_any = False
                        for fig in parsed_figs:
                            fig_id = str(fig.get("fig_id", ""))
                            fig_caption = str(fig.get("fig_caption", "")).strip()
                            graphic_ref = fig.get("graphic_ref", "")
                            if not fig_caption or not graphic_ref:
                                continue

                            base = Path(graphic_ref).name
                            stem = Path(base).stem
                            candidates = [base.lower()] + [(stem + ext).lower() for ext in (".jpg", ".jpeg", ".png", ".tif", ".tiff")]

                            member = None
                            for c in candidates:
                                if c in img_tar_members:
                                    member = img_tar_members[c]
                                    break
                            if member is None:
                                for mname in img_tar_members:
                                    if stem in mname:
                                        member = img_tar_members[mname]
                                        break
                            if member is None:
                                continue

                            pair_id = f"{_pmid}_{fig_id}"
                            if resume and pair_id in existing_pairs:
                                continue

                            img_bytes = _extract_member_to_bytes(tar, member)
                            img_rel  = Path(_pmc) / f"{_pmc}_fig-{fig_id}.jpg"
                            img_abs  = images_out_dir / img_rel

                            try:
                                saved_path = _ensure_jpeg_and_save(img_bytes, img_abs)
                            except Exception:
                                continue

                            record = {
                                "image": str(saved_path.relative_to(images_out_dir).as_posix()),
                                "caption": fig_caption,
                                "pmid": _pmid,
                                "pmcid": _pmc,
                                "pair_id": pair_id,
                            }
                            jf.write(json.dumps(record, ensure_ascii=False) + "\n")
                            existing_pairs.add(pair_id)
                            n_written += 1
                            wrote_any = True

                        if not wrote_any:
                            n_skipped += 1

            except requests.HTTPError:
                n_skipped += 1
                continue
            except tarfile.ReadError:
                n_skipped += 1
                continue
            except Exception:
                n_skipped += 1
                continue

            pbar.set_postfix(written=n_written, skipped=n_skipped)

    print(f"[{file_list_path.name}] Wrote {n_written} pairs. Skipped {n_skipped}.")

# =======================
# Runners for dev/test and shards
# =======================

def run_dev_and_test(
    dev_list: Path,
    test_list: Path,
    out_root: Path = repo_root / "_results/data/pmc15_cc12m_like",
):
    """
    Produces:
      out_root/
        dev.jsonl
        test.jsonl
        images/
          dev/PMCxxxx/*.jpg
          test/PMCxxxx/*.jpg
    """
    images_root = out_root / "images"
    # dev
    build_pubmed_image_caption_dataset_streaming(
        file_list_path=dev_list,
        jsonl_out_path=out_root / "dev.jsonl",
        images_out_dir=images_root / "dev",
        resume=True,
        use_dual_base=True,
    )
    # test
    build_pubmed_image_caption_dataset_streaming(
        file_list_path=test_list,
        jsonl_out_path=out_root / "test.jsonl",
        images_out_dir=images_root / "test",
        resume=True,
        use_dual_base=True,
    )

def run_train_shards(
    shard_lists: List[Path],   # e.g., [train_shard-00001.txt, train_shard-00002.txt, ...]
    out_root: Path = repo_root / "_results/data/pmc15_cc12m_like",
    shard_name_fmt: str = "train_shard-{idx:05d}",  # file/directory naming format
):
    """
    For each shard list, produce one JSONL and one images subfolder:
      out_root/
        train_shard-00001.jsonl
        train_shard-00002.jsonl
        ...
        images/train/shard-00001/PMCxxxx/*.jpg
        images/train/shard-00002/PMCxxxx/*.jpg
        ...
    """
    images_root = out_root / "images" / "train"
    for i, shard_list in enumerate(shard_lists, start=1):
        shard_name = shard_name_fmt.format(idx=i)
        jsonl_out  = out_root / f"{shard_name}.jsonl"
        images_dir = images_root / f"shard-{i:05d}"
        build_pubmed_image_caption_dataset_streaming(
            file_list_path=shard_list,
            jsonl_out_path=jsonl_out,
            images_out_dir=images_dir,
            resume=True,
            use_dual_base=True,
        )

# =======================
# CLI convenience (optional)
# =======================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build PMC15 CC12M-like dataset (dev/test/shards).")
    parser.add_argument("--mode", choices=["devtest", "shards"], required=True)
    parser.add_argument("--dev_list", type=Path, help="TSV list for dev split")
    parser.add_argument("--test_list", type=Path, help="TSV list for test split")
    parser.add_argument("--shard_dir", type=Path, help="Directory containing train shard TSVs")
    parser.add_argument("--out_root", type=Path, default=repo_root / "_results/data/pmc15_cc12m_like")
    args = parser.parse_args()

    if args.mode == "devtest":
        assert args.dev_list and args.test_list, "Provide --dev_list and --test_list"
        run_dev_and_test(args.dev_list, args.test_list, args.out_root)
    else:
        assert args.shard_dir and args.shard_dir.is_dir(), "Provide --shard_dir"
        shard_lists = sorted([p for p in args.shard_dir.glob("*.txt")])
        assert shard_lists, f"No shard list files found in {args.shard_dir}"
        run_train_shards(shard_lists, args.out_root)

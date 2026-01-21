import os
import json
import time
import shutil
import queue
import threading
import subprocess
import hashlib
import logging
from collections import defaultdict
from functools import wraps

import pydicom
from pydicom.fileset import FileSet
import SimpleITK as sitk
import nibabel as nib
import pandas as pd

# ======================================================
# CONFIG
# ======================================================
GDRIVE_DICOM = "gdrive:Medicina/Doutorado IDOR/Exames/DICOM"
GDRIVE_NIFTI = "gdrive:Medicina/Doutorado IDOR/Exames/NIfTI2"

WORKDIR = os.path.expanduser("~/work")
LOCAL_DICOM = os.path.join(WORKDIR, "dicom")
LOCAL_NIFTI = os.path.join(WORKDIR, "nifti")
PROGRESS_FILE = os.path.join(WORKDIR, "progress.json")

MIN_SLICES = 10
DOWNLOAD_THREADS = 2
UPLOAD_THREADS = 2

# ======================================================
# LOG
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("PIPELINE")

# ======================================================
# UTIL
# ======================================================
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "downloaded": [],
        "converted": [],
        "uploaded": []
    }

def save_progress(p):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(8192), b""):
            h.update(b)
    return h.hexdigest()

def rclone(cmd):
    subprocess.run(cmd, check=True)

# ======================================================
# DISCOVERY REMOTA
# ======================================================
def list_dicomdirs():
    cmd = [
        "rclone", "lsf", "--dirs-only", "--recursive",
        GDRIVE_DICOM
    ]
    out = subprocess.check_output(cmd, text=True)
    return [d.strip("/") for d in out.splitlines()]

# ======================================================
# DOWNLOAD
# ======================================================
def download_worker(q, progress):
    while True:
        item = q.get()
        if item is None:
            break

        if item in progress["downloaded"]:
            q.task_done()
            continue

        local = os.path.join(LOCAL_DICOM, item)
        os.makedirs(local, exist_ok=True)

        log.info(f"⬇️  Download: {item}")
        rclone([
            "rclone", "copy",
            f"{GDRIVE_DICOM}/{item}",
            local,
            "--transfers", "2",
            "--checkers", "2"
        ])

        progress["downloaded"].append(item)
        save_progress(progress)
        q.task_done()

# ======================================================
# PARSE + CONVERSÃO
# ======================================================
def parse_dicomdir(path):
    fs = FileSet(os.path.join(path, "DICOMDIR"))
    series = defaultdict(list)

    for inst in fs:
        if inst.DirectoryRecordType != "IMAGE":
            continue
        dcm = pydicom.dcmread(inst.path, stop_before_pixels=True)
        if dcm.Modality not in ("CT", "MR"):
            continue
        series[dcm.SeriesInstanceUID].append(inst.path)

    return series

def convert_series(files, out):
    reader = sitk.ImageSeriesReader()
    reader.SetFileNames(files)
    img = reader.Execute()
    sitk.WriteImage(img, out, True)

# ======================================================
# PIPELINE PRINCIPAL
# ======================================================
def run():
    os.makedirs(LOCAL_DICOM, exist_ok=True)
    os.makedirs(LOCAL_NIFTI, exist_ok=True)

    progress = load_progress()
    dicomdirs = list_dicomdirs()

    # DOWNLOAD
    dq = queue.Queue()
    for d in dicomdirs:
        dq.put(d)

    for _ in range(DOWNLOAD_THREADS):
        threading.Thread(
            target=download_worker,
            args=(dq, progress),
            daemon=True
        ).start()

    dq.join()

    # PROCESSAMENTO
    metadata = []

    for d in dicomdirs:
        if d in progress["converted"]:
            continue

        path = os.path.join(LOCAL_DICOM, d)
        series = parse_dicomdir(path)

        for uid, files in series.items():
            if len(files) < MIN_SLICES:
                continue

            out = os.path.join(
                LOCAL_NIFTI,
                f"{d}_{uid}.nii.gz"
            )

            log.info(f"🧠 Convertendo {uid}")
            convert_series(files, out)

            nii = nib.load(out)
            metadata.append({
                "dicomdir": d,
                "series_uid": uid,
                "shape": nii.shape,
                "spacing": nii.header.get_zooms(),
                "sha256": sha256(out)
            })

            # UPLOAD
            log.info(f"⬆️  Upload {out}")
            rclone([
                "rclone", "copy",
                out,
                GDRIVE_NIFTI,
                "--transfers", "2",
                "--checkers", "2"
            ])

            os.remove(out)

        shutil.rmtree(path)
        progress["converted"].append(d)
        save_progress(progress)

    # METADATA
    pd.DataFrame(metadata).to_csv(
        os.path.join(WORKDIR, "metadata.csv"),
        index=False
    )

    log.info("🏁 PIPELINE FINALIZADO")

# ======================================================
if __name__ == "__main__":
    run()

#!/usr/bin/env python3
# ======================================================
# DICOMDIR → NIfTI PIPELINE (PRODUÇÃO / PUBLICAÇÃO)
# ======================================================

import os
import sys
import json
import time
import shutil
import queue
import threading
import subprocess
import logging
import hashlib
from collections import defaultdict
from contextlib import contextmanager

import pydicom
import SimpleITK as sitk
import nibabel as nib
import pandas as pd
from tqdm import tqdm

import config  # arquivo config.py externo

# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("DICOMDIR_PIPELINE")

# ======================================================
# CONTEXTO DE TEMPO
# ======================================================
@contextmanager
def timed(label):
    start = time.time()
    yield
    elapsed = time.time() - start
    log.info(f"⏱ {label}: {elapsed:.2f}s")

# ======================================================
# UTILIDADES
# ======================================================
def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_progress():
    if os.path.exists(config.PROGRESS_FILE):
        with open(config.PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "listed": [],
        "downloaded": [],
        "converted": [],
        "uploaded": []
    }


def save_progress(progress):
    os.makedirs(os.path.dirname(config.PROGRESS_FILE), exist_ok=True)
    with open(config.PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# ======================================================
# DISCOVERY REMOTO (CACHE + RETOMADA)
# ======================================================
def list_remote_dicomdirs(progress):
    if progress["listed"]:
        log.info("📦 Usando cache local de DICOMDIRs")
        return progress["listed"]

    log.info("📂 Listando DICOMDIRs remotos...")
    cmd = ["rclone", "lsf", "-R", config.GDRIVE_DICOM]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

    dicomdirs = []
    for line in proc.stdout:
        path = line.strip()
        if path.upper().endswith("DICOMDIR"):
            dicomdirs.append(path)
            print(f"\r📂 {path}", end="", flush=True)

    print()
    progress["listed"] = dicomdirs
    save_progress(progress)

    log.info(f"✓ {len(dicomdirs)} DICOMDIRs encontrados")
    return dicomdirs


# ======================================================
# DOWNLOAD (PATCH CRÍTICO)
# ======================================================
def download_exam(remote_dicomdir, local_exam_dir):
    remote_exam_dir = os.path.dirname(remote_dicomdir)

    os.makedirs(local_exam_dir, exist_ok=True)

    subprocess.run(
        [
            "rclone", "copy",
            f"{config.GDRIVE_DICOM}/{remote_exam_dir}",
            local_exam_dir,
            "--ignore-existing",
            "--checksum",
            "--transfers", "4",
            "--checkers", "4"
        ],
        check=True
    )


# ======================================================
# DESCOBERTA LOCAL DE SÉRIES
# ======================================================
def discover_series(local_dir):
    reader = sitk.ImageSeriesReader()
    series_ids = reader.GetGDCMSeriesIDs(local_dir)
    return series_ids or []


# ======================================================
# CONVERSÃO DICOM → NIFTI
# ======================================================
def convert_series(local_dir, series_id, out_dir):
    reader = sitk.ImageSeriesReader()
    files = reader.GetGDCMSeriesFileNames(local_dir, series_id)

    if len(files) < config.MIN_SLICES:
        return None

    reader.SetFileNames(files)
    img = reader.Execute()

    img = sitk.Cast(img, sitk.sitkInt16)

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{series_id}.nii.gz")

    sitk.WriteImage(img, out_path, True)
    return out_path


# ======================================================
# PIPELINE DE CONVERSÃO LOCAL
# ======================================================
def convert_local_exam(exam_id, progress):
    local_exam_dir = os.path.join(config.LOCAL_DICOM, exam_id)
    out_exam_dir = os.path.join(config.LOCAL_NIFTI, exam_id)

    series_ids = discover_series(local_exam_dir)

    if not series_ids:
        log.warning(f"⚠️ Nenhuma série em {exam_id}")
        return

    for sid in series_ids:
        if sid in progress["converted"]:
            continue

        with timed(f"Conversão série {sid}"):
            out = convert_series(local_exam_dir, sid, out_exam_dir)
            if out:
                progress["converted"].append(sid)
                save_progress(progress)
                log.info(f"✅ Série convertida: {sid}")


# ======================================================
# UPLOAD NIFTI
# ======================================================
def upload_exam(exam_id):
    local = os.path.join(config.LOCAL_NIFTI, exam_id)
    remote = f"{config.GDRIVE_NIFTI}/{exam_id}"

    subprocess.run(
        ["rclone", "copy", local, remote, "--ignore-existing"],
        check=True
    )


# ======================================================
# WORKERS
# ======================================================
def downloader(queue_in, progress):
    while True:
        item = queue_in.get()
        if item is None:
            break

        exam_id, remote_dicomdir = item

        if exam_id not in progress["downloaded"]:
            with timed(f"Download {exam_id}"):
                download_exam(remote_dicomdir, os.path.join(config.LOCAL_DICOM, exam_id))
                progress["downloaded"].append(exam_id)
                save_progress(progress)

        queue_in.task_done()


def converter(queue_in, progress):
    while True:
        exam_id = queue_in.get()
        if exam_id is None:
            break

        convert_local_exam(exam_id, progress)

        if exam_id not in progress["uploaded"]:
            with timed(f"Upload {exam_id}"):
                upload_exam(exam_id)
                progress["uploaded"].append(exam_id)
                save_progress(progress)

        queue_in.task_done()


# ======================================================
# MAIN
# ======================================================
def run():
    os.makedirs(config.LOCAL_DICOM, exist_ok=True)
    os.makedirs(config.LOCAL_NIFTI, exist_ok=True)

    progress = load_progress()
    dicomdirs = list_remote_dicomdirs(progress)

    q_download = queue.Queue()
    q_convert = queue.Queue()

    for i in range(config.DOWNLOAD_THREADS):
        threading.Thread(target=downloader, args=(q_download, progress), daemon=True).start()

    for i in range(config.CONVERSION_THREADS):
        threading.Thread(target=converter, args=(q_convert, progress), daemon=True).start()

    for idx, dcm in enumerate(tqdm(dicomdirs, desc="📂 DICOMDIRs", unit="dir")):
        exam_id = str(idx)

        if exam_id not in progress["downloaded"]:
            q_download.put((exam_id, dcm))

        q_convert.put(exam_id)

    q_download.join()
    q_convert.join()

    log.info("🏁 PIPELINE FINALIZADO")


if __name__ == "__main__":
    run()

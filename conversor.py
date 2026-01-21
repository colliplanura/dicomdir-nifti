#!/usr/bin/env python3

import os
import json
import time
import threading
import queue
import subprocess
import logging
from collections import defaultdict

import pydicom
from pydicom.fileset import FileSet
import SimpleITK as sitk
from tqdm import tqdm
import config

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
# PROGRESS
# ======================================================
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


def save_progress(p):
    os.makedirs(os.path.dirname(config.PROGRESS_FILE), exist_ok=True)
    with open(config.PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)

# ======================================================
# DISCOVERY REMOTO
# ======================================================
def list_dicomdirs(progress):
    if progress["listed"]:
        return progress["listed"]

    log.info("📂 Listando DICOMDIRs remotos...")
    cmd = ["rclone", "lsf", "-R", config.GDRIVE_DICOM]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

    dicomdirs = []
    for line in proc.stdout:
        p = line.strip()
        if p.upper().endswith("DICOMDIR"):
            dicomdirs.append(p)
            print(f"\r📂 {p}", end="", flush=True)

    print()
    progress["listed"] = dicomdirs
    save_progress(progress)
    log.info(f"✓ {len(dicomdirs)} DICOMDIRs encontrados")
    return dicomdirs

# ======================================================
# DOWNLOAD (EXAME COMPLETO)
# ======================================================
def download_exam(remote_dicomdir, local_exam_dir):
    remote_exam_dir = os.path.dirname(remote_dicomdir)

    subprocess.run(
        [
            "rclone", "copy",
            f"{config.GDRIVE_DICOM}/{remote_exam_dir}",
            local_exam_dir,
            "--ignore-existing",
            "--checksum"
        ],
        check=True
    )

# ======================================================
# CONVERSÃO
# ======================================================

def convert_exam(local_exam_dir, local_nifti_dir):
    dicomdir_path = os.path.join(local_exam_dir, "DICOMDIR")

    if not os.path.exists(dicomdir_path):
        log.error("❌ DICOMDIR não encontrado")
        return False

    fs = FileSet(dicomdir_path)
    series_map = defaultdict(list)

    # ============================
    # DISCOVERY REAL DAS IMAGENS
    # ============================
    for rec in fs:
        if rec.DirectoryRecordType != "IMAGE":
            continue

        dcm_path = rec.path
        try:
            ds = pydicom.dcmread(dcm_path, stop_before_pixels=True)
        except Exception:
            continue

        if "SeriesInstanceUID" not in ds:
            continue

        series_map[ds.SeriesInstanceUID].append(dcm_path)

    if not series_map:
        log.warning("⛔ Nenhuma série encontrada via DICOMDIR")
        return False

    os.makedirs(local_nifti_dir, exist_ok=True)
    converted_any = False

    # ============================
    # CONVERSÃO
    # ============================
    for uid, files in series_map.items():
        if len(files) < config.MIN_SLICES:
            continue

        try:
            reader = sitk.ImageSeriesReader()
            reader.SetFileNames(files)
            img = reader.Execute()

            out = os.path.join(local_nifti_dir, f"{uid}.nii.gz")
            sitk.WriteImage(img, out, True)

            log.info(f"✅ Série convertida: {uid} ({len(files)} slices)")
            converted_any = True

        except Exception as e:
            log.error(f"❌ Erro na série {uid}: {e}")

    return converted_any


# ======================================================
# UPLOAD
# ======================================================
def upload_exam(local_nifti_dir, exam_id):
    if not os.path.exists(local_nifti_dir):
        return False

    if not os.listdir(local_nifti_dir):
        return False

    subprocess.run(
        [
            "rclone", "copy",
            local_nifti_dir,
            f"{config.GDRIVE_NIFTI}/{exam_id}",
            "--ignore-existing"
        ],
        check=True
    )
    return True

# ======================================================
# MAIN
# ======================================================
def run():
    os.makedirs(config.LOCAL_DICOM, exist_ok=True)
    os.makedirs(config.LOCAL_NIFTI, exist_ok=True)

    progress = load_progress()
    dicomdirs = list_dicomdirs(progress)

    for dcm in tqdm(dicomdirs, desc="📂 DICOMDIRs", unit="dir"):
        exam_remote_dir = os.path.dirname(dcm)
        exam_id = exam_remote_dir.replace("/", "_")

        local_exam_dir = os.path.join(config.LOCAL_DICOM, exam_id)
        local_nifti_dir = os.path.join(config.LOCAL_NIFTI, exam_id)

        if exam_id not in progress["downloaded"]:
            log.info(f"⬇️ Download exame {exam_id}")
            download_exam(dcm, local_exam_dir)
            progress["downloaded"].append(exam_id)
            save_progress(progress)

        if exam_id not in progress["converted"]:
            log.info(f"🧠 Convertendo exame {exam_id}")
            ok = convert_exam(local_exam_dir, local_nifti_dir)
            if ok:
                progress["converted"].append(exam_id)
                save_progress(progress)
            else:
                continue

        if exam_id not in progress["uploaded"]:
            log.info(f"☁️ Upload exame {exam_id}")
            if upload_exam(local_nifti_dir, exam_id):
                progress["uploaded"].append(exam_id)
                save_progress(progress)

    log.info("🏁 PIPELINE FINALIZADO")

if __name__ == "__main__":
    run()

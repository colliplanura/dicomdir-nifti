#!/usr/bin/env python3

import os
import json
import threading
import queue
import subprocess
import logging
import shutil
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
# GLOBAL LOCKS
# ======================================================
progress_lock = threading.Lock()
in_progress = set()

# ======================================================
# PROGRESS
# ======================================================
def load_progress():
    if os.path.exists(config.PROGRESS_FILE):
        with open(config.PROGRESS_FILE) as f:
            p = json.load(f)
    else:
        p = {}

    # garante chaves
    for k in ("listed", "downloaded", "converted", "uploaded"):
        p.setdefault(k, [])

    return p


def save_progress(p):
    with progress_lock:
        os.makedirs(os.path.dirname(config.PROGRESS_FILE), exist_ok=True)
        with open(config.PROGRESS_FILE, "w") as f:
            json.dump(p, f, indent=2)

# ======================================================
# DISCOVERY REMOTO (COM RESUME)
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
# DOWNLOAD
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

    # ----------------------------
    # DISCOVERY REAL VIA DICOMDIR
    # ----------------------------
    for rec in fs:
        if rec.DirectoryRecordType != "IMAGE":
            continue

        dcm_path = rec.path
        try:
            ds = pydicom.dcmread(dcm_path, stop_before_pixels=True)
        except Exception:
            continue

        uid = getattr(ds, "SeriesInstanceUID", None)
        if not uid:
            continue

        series_map[uid].append(dcm_path)

    if not series_map:
        log.warning("⛔ Nenhuma série encontrada via DICOMDIR")
        return False

    os.makedirs(local_nifti_dir, exist_ok=True)
    converted_any = False

    # ----------------------------
    # CONVERSÃO POR SÉRIE
    # ----------------------------
    for uid, files in series_map.items():
        if len(files) < config.MIN_SLICES:
            continue

        try:
            # ordenação robusta por posição Z
            def zpos(f):
                ds = pydicom.dcmread(f, stop_before_pixels=True)
                ipp = ds.get("ImagePositionPatient", [0, 0, 0])
                return float(ipp[2])

            files = sorted(files, key=zpos)

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
# PROCESSAMENTO ATÔMICO POR EXAME
# ======================================================
def process_exam(dcm, progress):
    exam_remote_dir = os.path.dirname(dcm)
    exam_id = exam_remote_dir.replace("/", "_")

    with progress_lock:
        if exam_id in in_progress:
            return
        in_progress.add(exam_id)

    try:
        local_exam_dir = os.path.join(config.LOCAL_DICOM, exam_id)
        local_nifti_dir = os.path.join(config.LOCAL_NIFTI, exam_id)

        # DOWNLOAD
        if exam_id not in progress["downloaded"]:
            log.info(f"⬇️ [{exam_id}] Download")
            download_exam(dcm, local_exam_dir)
            progress["downloaded"].append(exam_id)
            save_progress(progress)

        # CONVERSÃO
        if exam_id not in progress["converted"]:
            log.info(f"🧠 [{exam_id}] Conversão")
            ok = convert_exam(local_exam_dir, local_nifti_dir)
            if not ok:
                return
            progress["converted"].append(exam_id)
            save_progress(progress)

        # UPLOAD
        if exam_id not in progress["uploaded"]:
            log.info(f"☁️ [{exam_id}] Upload")
            upload_exam(local_nifti_dir, exam_id)
            progress["uploaded"].append(exam_id)
            save_progress(progress)

        log.info(f"✅ [{exam_id}] Finalizado")

    except Exception as e:
        log.error(f"❌ [{exam_id}] Falha: {e}")

    finally:
        with progress_lock:
            in_progress.discard(exam_id)

# ======================================================
# WORKER
# ======================================================
def worker(q, progress):
    while True:
        try:
            dcm = q.get(timeout=3)
        except queue.Empty:
            return

        process_exam(dcm, progress)
        q.task_done()

# ======================================================
# MAIN
# ======================================================
def run():
    # check external dependency
    if shutil.which("rclone") is None:
        log.error("rclone não encontrado. Instale o rclone e certifique-se de que está no PATH.")
        raise SystemExit(1)

    os.makedirs(config.LOCAL_DICOM, exist_ok=True)
    os.makedirs(config.LOCAL_NIFTI, exist_ok=True)

    progress = load_progress()
    dicomdirs = list_dicomdirs(progress)

    q = queue.Queue(maxsize=config.QUEUE_SIZE)

    # workers
    threads = []
    for _ in range(config.EXAM_WORKERS):
        t = threading.Thread(target=worker, args=(q, progress), daemon=True)
        t.start()
        threads.append(t)

    # producer
    for dcm in tqdm(dicomdirs, desc="📂 DICOMDIRs", unit="dir"):
        q.put(dcm)

    q.join()
    log.info("🏁 PIPELINE FINALIZADO")

if __name__ == "__main__":
    run()

import os
import json
import subprocess
import threading
import queue
import time
import logging
from tqdm import tqdm

import config


# ======================================================
# LOGGING
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
        "listed_index": 0,
        "dicomdirs": {},
        "converted": {},
        "uploaded": {}
    }


def save_progress(p):
    with open(config.PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)


# ======================================================
# DISCOVERY REMOTO (RETOMÁVEL)
# ======================================================
def list_remote_dicomdirs(progress):
    if os.path.exists(config.REMOTE_INDEX_FILE):
        with open(config.REMOTE_INDEX_FILE) as f:
            return json.load(f)

    log.info("📡 Listando DICOMDIRs remotos...")
    dicomdirs = []

    cmd = ["rclone", "lsf", "-R", config.GDRIVE_DICOM]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

    for idx, line in enumerate(proc.stdout):
        if idx < progress["listed_index"]:
            continue

        path = line.strip()
        print(f"\r📡 {path}", end="", flush=True)

        if path.upper().endswith("DICOMDIR"):
            dicomdirs.append(os.path.dirname(path))

        progress["listed_index"] = idx
        save_progress(progress)

    print()
    with open(config.REMOTE_INDEX_FILE, "w") as f:
        json.dump(dicomdirs, f, indent=2)

    return dicomdirs


# ======================================================
# DOWNLOAD
# ======================================================
def download_dicomdir(remote_path, progress):
    local_path = os.path.join(config.LOCAL_DICOM, remote_path.replace("/", "_"))

    if progress["dicomdirs"].get(remote_path) == "downloaded":
        return local_path

    os.makedirs(local_path, exist_ok=True)

    cmd = [
        "rclone", "copy",
        f"{config.GDRIVE_DICOM}/{remote_path}",
        local_path,
        "--progress"
    ]
    subprocess.run(cmd, check=True)

    progress["dicomdirs"][remote_path] = "downloaded"
    save_progress(progress)

    return local_path


# ======================================================
# CONVERSÃO (CHAMA SCRIPT JÁ VALIDADO)
# ======================================================
def convert_local_dicom(local_path, progress):
    if progress["converted"].get(local_path):
        return

    # Aqui você chama o pipeline DICOMDIR→NIfTI que já validamos
    subprocess.run(
        ["python", "dicomdir_to_nifti.py", local_path, config.LOCAL_NIFTI],
        check=True
    )

    progress["converted"][local_path] = True
    save_progress(progress)


# ======================================================
# UPLOAD
# ======================================================
def upload_nifti(progress):
    for f in os.listdir(config.LOCAL_NIFTI):
        if f in progress["uploaded"]:
            continue

        local = os.path.join(config.LOCAL_NIFTI, f)
        cmd = [
            "rclone", "copyto",
            local,
            f"{config.GDRIVE_NIFTI}/{f}",
            "--progress"
        ]
        subprocess.run(cmd, check=True)

        progress["uploaded"][f] = True
        save_progress(progress)


# ======================================================
# PIPELINE PRINCIPAL
# ======================================================
def run():
    os.makedirs(config.LOCAL_DICOM, exist_ok=True)
    os.makedirs(config.LOCAL_NIFTI, exist_ok=True)

    progress = load_progress()

    dicomdirs = list_remote_dicomdirs(progress)

    for d in tqdm(dicomdirs, desc="📂 DICOMDIRs"):
        local = download_dicomdir(d, progress)
        convert_local_dicom(local, progress)

    upload_nifti(progress)

    log.info("🏁 PIPELINE FINALIZADO COM SUCESSO")


if __name__ == "__main__":
    run()

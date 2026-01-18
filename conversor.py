import os
import json
import time
import hashlib
import logging
from collections import defaultdict

import pydicom
import SimpleITK as sitk
import nibabel as nib
import pandas as pd


# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("NEURO_PIPELINE")


# ======================================================
# CONFIG
# ======================================================
PROGRESS_FILE = "progress.json"


# ======================================================
# UTILIDADES
# ======================================================
def sanitize(text):
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in str(text))


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def is_dicom(path):
    try:
        pydicom.dcmread(path, stop_before_pixels=True)
        return True
    except Exception:
        return False


# ======================================================
# DISCOVERY
# ======================================================
def find_dicomdirs(root):
    dicomdirs = []
    for r, _, fs in os.walk(root):
        for f in fs:
            if f.upper() == "DICOMDIR":
                dicomdirs.append(os.path.join(r, f))
    return sorted(dicomdirs)


def find_dicom_files(base_dir):
    files = []
    for r, _, fs in os.walk(base_dir):
        for f in fs:
            p = os.path.join(r, f)
            if f.upper() != "DICOMDIR" and is_dicom(p):
                files.append(p)
    return files


# ======================================================
# FILTROS CLÍNICOS
# ======================================================
def is_valid_volume(dcm):
    desc = getattr(dcm, "SeriesDescription", "").upper()
    modality = getattr(dcm, "Modality", "").upper()

    blacklist = ["SCOUT", "LOCALIZER", "LOC", "TOPOGRAM"]

    if modality not in ("CT", "MR"):
        return False

    if any(b in desc for b in blacklist):
        return False

    return True


# ======================================================
# CONVERSÃO ROBUSTA
# ======================================================
def dicom_series_to_nifti(files):
    reader = sitk.ImageSeriesReader()
    reader.SetFileNames(files)
    img = reader.Execute()

    img = sitk.Cast(img, sitk.sitkInt16)

    fixed = sitk.Image(img)
    fixed.SetSpacing(img.GetSpacing())
    fixed.SetOrigin(img.GetOrigin())
    fixed.SetDirection(img.GetDirection())

    return fixed


# ======================================================
# CHECKPOINT
# ======================================================
def load_progress(output_dir):
    path = os.path.join(output_dir, PROGRESS_FILE)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {"completed": []}


def save_progress(output_dir, progress):
    path = os.path.join(output_dir, PROGRESS_FILE)
    with open(path, "w") as f:
        json.dump(progress, f, indent=2)


# ======================================================
# PROCESSA UM DICOMDIR
# ======================================================
def process_dicomdir(dicomdir, output_dir, metadata_rows):
    base_dir = os.path.dirname(dicomdir)
    log.info(f"📂 Processando DICOMDIR: {dicomdir}")

    files = find_dicom_files(base_dir)
    log.info(f"📁 DICOMs encontrados: {len(files)}")

    series = defaultdict(list)
    meta = {}

    for f in files:
        try:
            dcm = pydicom.dcmread(f, stop_before_pixels=True)
            if not is_valid_volume(dcm):
                continue

            uid = sanitize(dcm.SeriesInstanceUID)
            series[uid].append(f)

            meta[uid] = {
                "patient_id": sanitize(getattr(dcm, "PatientID", "UNKNOWN")),
                "modality": dcm.Modality,
                "study_uid": dcm.StudyInstanceUID
            }
        except Exception:
            continue

    log.info(f"🧠 Séries válidas: {len(series)}")

    for uid, files in series.items():
        if len(files) < 10:
            continue

        files.sort(
            key=lambda x: float(
                pydicom.dcmread(
                    x, stop_before_pixels=True
                ).get("ImagePositionPatient", [0, 0, 0])[2]
            )
        )

        img = dicom_series_to_nifti(files)

        name = f"{meta[uid]['patient_id']}_{meta[uid]['modality']}_{uid}.nii.gz"
        out = os.path.join(output_dir, name)

        sitk.WriteImage(img, out, True)

        nii = nib.load(out)

        metadata_rows.append({
            "filename": name,
            "patient_id": meta[uid]["patient_id"],
            "modality": meta[uid]["modality"],
            "series_uid": uid,
            "study_uid": meta[uid]["study_uid"],
            "shape": nii.shape,
            "spacing": nii.header.get_zooms(),
            "sha256": sha256(out)
        })

        log.info(f"✅ Série convertida: {name}")


# ======================================================
# PIPELINE PRINCIPAL (COM RESUME)
# ======================================================
def run_pipeline(root_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    progress = load_progress(output_dir)
    completed = set(progress["completed"])

    dicomdirs = find_dicomdirs(root_dir)
    log.info(f"🔎 DICOMDIRs encontrados: {len(dicomdirs)}")

    metadata_rows = []

    for idx, dicomdir in enumerate(dicomdirs, 1):
        if dicomdir in completed:
            log.info(f"⏭️ [{idx}/{len(dicomdirs)}] Já processado — pulando")
            continue

        log.info(f"\n===== [{idx}/{len(dicomdirs)}] NOVO DICOMDIR =====")

        try:
            process_dicomdir(dicomdir, output_dir, metadata_rows)

            completed.add(dicomdir)
            progress["completed"] = list(completed)
            save_progress(output_dir, progress)

            log.info("💾 Checkpoint salvo")

        except Exception as e:
            log.error(f"❌ Erro no DICOMDIR {dicomdir}: {e}")
            log.info("⚠️ Execução pode ser retomada depois")
            break

    # Salvar metadados finais
    if metadata_rows:
        csv_path = os.path.join(output_dir, "metadata.csv")
        pd.DataFrame(metadata_rows).to_csv(csv_path, index=False)
        log.info(f"📄 Metadados exportados: {csv_path}")

    log.info("🏁 PIPELINE FINALIZADO")


# ======================================================
# EXECUÇÃO
# ======================================================
if __name__ == "__main__":
    ROOT = "/content/drive/MyDrive/Medicina/Doutorado IDOR/Exames/DICOM"
    OUT = "/content/drive/MyDrive/Medicina/Doutorado IDOR/Exames/NIfTI_PUBLICAVEL"

    run_pipeline(ROOT, OUT)

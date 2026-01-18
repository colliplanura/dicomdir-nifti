import os
import time
import csv
import hashlib
import logging
from collections import defaultdict

import pydicom
import SimpleITK as sitk
import nibabel as nib
import numpy as np
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
# DESCOBERTA DE DICOM
# ======================================================
def find_dicom_files(root):
    files = []
    for r, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(r, f)
            if f.upper() != "DICOMDIR" and is_dicom(p):
                files.append(p)
    return files


# ======================================================
# FILTRAGEM DE SÉRIES NÃO VOLUMÉTRICAS
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
# VALIDAÇÃO DE DIMENSÕES
# ======================================================
def get_image_dimensions(dicom_path):
    """Retorna as dimensões (linhas, colunas) de uma imagem DICOM."""
    try:
        dcm = pydicom.dcmread(dicom_path, stop_before_pixels=True)
        rows = int(getattr(dcm, "Rows", 0))
        cols = int(getattr(dcm, "Columns", 0))
        return (rows, cols)
    except Exception:
        return None


def filter_consistent_dimensions(files):
    """Filtra arquivos mantendo apenas aqueles com a dimensão mais comum."""
    dimensions_count = defaultdict(list)
    
    for f in files:
        dims = get_image_dimensions(f)
        if dims:
            dimensions_count[dims].append(f)
    
    if not dimensions_count:
        return files, None
    
    # Encontra a dimensão mais comum
    most_common_dim = max(dimensions_count.keys(), key=lambda d: len(dimensions_count[d]))
    filtered_files = dimensions_count[most_common_dim]
    
    removed_count = len(files) - len(filtered_files)
    
    return filtered_files, (removed_count, most_common_dim)


# ======================================================
# CONVERSÃO ROBUSTA DICOM → NIFTI
# ======================================================
def dicom_series_to_nifti(files):
    reader = sitk.ImageSeriesReader()
    reader.SetFileNames(files)
    img = reader.Execute()

    # Forçar consistência científica
    img = sitk.Cast(img, sitk.sitkInt16)

    spacing = img.GetSpacing()
    origin = img.GetOrigin()
    direction = img.GetDirection()

    fixed = sitk.Image(img)
    fixed.SetSpacing(spacing)
    fixed.SetOrigin(origin)
    fixed.SetDirection(direction)

    return fixed


# ======================================================
# PIPELINE PRINCIPAL
# ======================================================
def run_pipeline(root_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "metadata.csv")

    dicoms = find_dicom_files(root_dir)
    log.info(f"📁 DICOMs encontrados: {len(dicoms)}")

    series = defaultdict(list)
    meta = {}

    for f in dicoms:
        try:
            dcm = pydicom.dcmread(f, stop_before_pixels=True)
            if not is_valid_volume(dcm):
                continue

            uid = sanitize(dcm.SeriesInstanceUID)
            series[uid].append(f)

            meta[uid] = {
                "patient_id": sanitize(getattr(dcm, "PatientID", "UNKNOWN")),
                "modality": dcm.Modality,
                "series_description": getattr(dcm, "SeriesDescription", ""),
                "study_uid": dcm.StudyInstanceUID
            }

        except Exception:
            continue

    log.info(f"🧠 Séries volumétricas válidas: {len(series)}")

    rows = []

    for i, (uid, files) in enumerate(series.items(), 1):
        if len(files) < 10:
            continue

        log.info(f"▶️ [{i}/{len(series)}] Série {uid} ({len(files)} imagens)")

        # Filtrar imagens com dimensões consistentes
        files, filter_info = filter_consistent_dimensions(files)
        
        if filter_info:
            removed, dims = filter_info
            log.warning(f"⚠️  {removed} imagem(ns) removida(s) por dimensões inconsistentes. Mantendo: {dims}")
        
        if len(files) < 10:
            log.warning(f"⚠️  Série ignorada: menos de 10 imagens após filtragem")
            continue

        files.sort(
            key=lambda x: float(
                pydicom.dcmread(
                    x, stop_before_pixels=True
                ).get("ImagePositionPatient", [0, 0, 0])[2]
            )
        )

        img = dicom_series_to_nifti(files)

        name = (
            f"{meta[uid]['patient_id']}_"
            f"{meta[uid]['modality']}_"
            f"{uid}.nii.gz"
        )

        out = os.path.join(output_dir, name)
        sitk.WriteImage(img, out, True)

        nii = nib.load(out)

        rows.append({
            "filename": name,
            "patient_id": meta[uid]["patient_id"],
            "modality": meta[uid]["modality"],
            "series_uid": uid,
            "study_uid": meta[uid]["study_uid"],
            "shape": nii.shape,
            "spacing": nii.header.get_zooms(),
            "sha256": sha256(out)
        })

        log.info(f"✅ Salvo: {name}")

    pd.DataFrame(rows).to_csv(csv_path, index=False)
    log.info(f"📄 Metadados exportados: {csv_path}")
    log.info("🏁 PIPELINE FINALIZADO COM SUCESSO")


# ======================================================
# EXECUÇÃO
# ======================================================
if __name__ == "__main__":
    ROOT = r"C:\Users\F8944859\Downloads\DICOM"
    OUT = r"C:\Users\F8944859\Downloads\NIfTI"

    run_pipeline(ROOT, OUT)

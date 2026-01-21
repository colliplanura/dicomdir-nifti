import os

# ======================================================
# CONFIG
# ======================================================

GDRIVE_DICOM = "gdrive:Medicina/Doutorado IDOR/Exames/DICOM"
GDRIVE_NIFTI = "gdrive:Medicina/Doutorado IDOR/Exames/teste/NIfTI"

WORKDIR = os.path.expanduser("~/teste")

LOCAL_DICOM = os.path.join(WORKDIR, "dicom")
LOCAL_NIFTI = os.path.join(WORKDIR, "nifti")

PROGRESS_FILE = os.path.join(WORKDIR, "progress.json")
DICOMDIR_CACHE = os.path.join(WORKDIR, "dicomdirs_cache.json")
METADATA_FILE = os.path.join(WORKDIR, "metadata.csv")

MIN_SLICES = 10

DOWNLOAD_THREADS = 2
CONVERSION_THREADS = 2

"""
======================================================
CONFIGURAÇÃO GLOBAL — DICOMDIR → NIFTI PIPELINE
======================================================

⚠️ Este arquivo NÃO deve conter lógica.
Apenas parâmetros de configuração.

Compatível com:
- Execução local / WSL
- Google Cloud VM
- PACS com DICOMDIR real
"""

import os

# ======================================================
# GOOGLE DRIVE / RCLONE
# ======================================================

# Remote com os DICOMDIRs
# Exemplo: "gdrive:PACS/DICOM"
GDRIVE_DICOM = "gdrive:Medicina/Doutorado IDOR/Exames/DICOM"

# Remote destino dos NIFTIs
# Exemplo: "gdrive:PACS/NIFTI"
GDRIVE_NIFTI = "gdrive:Medicina/Doutorado IDOR/Exames/testeMAc/NIfTI"

# ======================================================
# DIRETÓRIOS LOCAIS
# ======================================================

# Base local (pode ser alterado para SSD rápido)
LOCAL_BASE = os.environ.get(
    "DICOM_PIPELINE_BASE",
    os.path.expanduser("~/teste")
)

# DICOMs baixados (1 pasta por exame)
LOCAL_DICOM = os.path.join(LOCAL_BASE, "dicom")

# NIFTIs gerados (1 pasta por exame)
LOCAL_NIFTI = os.path.join(LOCAL_BASE, "nifti")

# ======================================================
# CONTROLE DE PIPELINE / RESUME
# ======================================================

# Arquivo de progresso (JSON)
PROGRESS_FILE = os.path.join(LOCAL_BASE, "progress", "progress.json")

# ======================================================
# PARALELIZAÇÃO
# ======================================================

# Quantos exames em paralelo
# ⚠️ Seguro: cada worker processa 1 exame inteiro
EXAM_WORKERS = int(os.environ.get("DICOM_EXAM_WORKERS", 2))

# Tamanho máximo da fila
QUEUE_SIZE = int(os.environ.get("DICOM_QUEUE_SIZE", 10))

# ======================================================
# CONVERSÃO DICOM → NIFTI
# ======================================================

# Mínimo de slices para considerar uma série válida
# Evita converter scout / localizer
MIN_SLICES = int(os.environ.get("DICOM_MIN_SLICES", 8))

# ======================================================
# LIMPEZA / RETENÇÃO (FUTURO)
# ======================================================

# Manter DICOMs locais após upload
KEEP_LOCAL_DICOM = False

# Manter NIFTIs locais após upload
KEEP_LOCAL_NIFTI = False

# ======================================================
# DEBUG / LOG
# ======================================================

# Ativar modo verbose (aumentar nível do log)
DEBUG = True

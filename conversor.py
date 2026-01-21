import os
import json
import time
import shutil
import queue
import threading
import subprocess
import hashlib
import logging
import signal
import sys
from collections import defaultdict
from functools import wraps

import pydicom
from pydicom.fileset import FileSet
import SimpleITK as sitk
import nibabel as nib
import pandas as pd
from tqdm import tqdm

# ======================================================
# CONFIG
# ======================================================
def load_config():
    """Carrega configurações do arquivo config.json"""
    config_file = os.path.join(os.path.dirname(__file__), "config.json")
    
    # Configurações padrão
    default_config = {
        "gdrive": {
            "dicom_path": "gdrive:Medicina/Doutorado IDOR/Exames/DICOM",
            "nifti_path": "gdrive:Medicina/Doutorado IDOR/Exames/NIfTI2"
        },
        "local": {
            "workdir": "~/work",
            "dicom_subdir": "dicom",
            "nifti_subdir": "nifti",
            "progress_file": "progress.json"
        },
        "processing": {
            "min_slices": 10,
            "download_threads": 2,
            "conversion_threads": 2
        }
    }
    
    # Tenta carregar do arquivo, senão usa padrão
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # Merge com default para garantir que todos os campos existam
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                    elif isinstance(default_config[key], dict):
                        for subkey in default_config[key]:
                            if subkey not in config[key]:
                                config[key][subkey] = default_config[key][subkey]
                return config
        except Exception as e:
            log.warning(f"⚠️  Erro ao ler config.json: {e}. Usando valores padrão.")
            return default_config
    else:
        log.info("ℹ️  Arquivo config.json não encontrado. Usando valores padrão.")
        return default_config

# Carrega configurações
CONFIG = load_config()

GDRIVE_DICOM = CONFIG["gdrive"]["dicom_path"]
GDRIVE_NIFTI = CONFIG["gdrive"]["nifti_path"]

WORKDIR = os.path.expanduser(CONFIG["local"]["workdir"])
LOCAL_DICOM = os.path.join(WORKDIR, CONFIG["local"]["dicom_subdir"])
LOCAL_NIFTI = os.path.join(WORKDIR, CONFIG["local"]["nifti_subdir"])
PROGRESS_FILE = os.path.join(WORKDIR, CONFIG["local"]["progress_file"])

MIN_SLICES = CONFIG["processing"]["min_slices"]
DOWNLOAD_THREADS = CONFIG["processing"]["download_threads"]
CONVERSION_THREADS = CONFIG["processing"]["conversion_threads"]

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
            data = json.load(f)
            # Migração: adiciona campo uploaded_files se não existir
            if "uploaded_files" not in data:
                data["uploaded_files"] = []
            return data
    return {
        "downloaded": [],
        "converted": [],
        "uploaded": [],
        "uploaded_files": []  # Lista de arquivos NIfTI já enviados
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

def check_remote_file_exists(remote_path):
    """Verifica se um arquivo existe no remote usando rclone"""
    try:
        result = subprocess.run(
            ["rclone", "lsf", remote_path],
            capture_output=True,
            text=True,
            check=False
        )
        return result.returncode == 0 and result.stdout.strip() != ""
    except Exception:
        return False

def recover_local_nifti_files():
    """Procura por arquivos NIfTI já convertidos localmente"""
    recovered = []
    if os.path.exists(LOCAL_NIFTI):
        for filename in os.listdir(LOCAL_NIFTI):
            if filename.endswith(".nii.gz"):
                recovered.append(filename)
    return recovered

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
def download_worker(q_download, q_convert, progress, pbar_download, progress_lock):
    while True:
        item = q_download.get()
        if item is None:
            break

        with progress_lock:
            already_downloaded = item in progress["downloaded"]
        
        if already_downloaded:
            pbar_download.update(1)
            q_convert.put(item)
            q_download.task_done()
            continue

        local = os.path.join(LOCAL_DICOM, item)
        os.makedirs(local, exist_ok=True)

        try:
            pbar_download.set_description(f"⬇️  {item[:40]}")
            rclone([
                "rclone", "copy",
                f"{GDRIVE_DICOM}/{item}",
                local,
                "--transfers", "2",
                "--checkers", "2",
                "--progress"
            ])

            with progress_lock:
                progress["downloaded"].append(item)
                save_progress(progress)
            
            pbar_download.update(1)
            
            # Envia para fila de conversão
            q_convert.put(item)
            
        except Exception as e:
            log.error(f"❌ Erro no download de {item}: {e}")
            # Não marca como baixado para tentar novamente
        
        q_download.task_done()

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
# CONVERSÃO + UPLOAD
# ======================================================
def conversion_worker(q_convert, progress, metadata_list, pbar_convert, progress_lock, metadata_lock):
    while True:
        item = q_convert.get()
        if item is None:
            break

        with progress_lock:
            if item in progress["converted"]:
                pbar_convert.update(1)
                q_convert.task_done()
                continue

        path = os.path.join(LOCAL_DICOM, item)
        
        try:
            pbar_convert.set_description(f"📁 {item[:30]}")
            series = parse_dicomdir(path)
            
            # Filtrar séries válidas
            valid_series = {uid: files for uid, files in series.items() 
                          if len(files) >= MIN_SLICES}
            
            if valid_series:
                for uid, files in valid_series.items():
                    nifti_filename = f"{item}_{uid}.nii.gz"
                    out = os.path.join(LOCAL_NIFTI, nifti_filename)
                    remote_file = f"{GDRIVE_NIFTI}/{nifti_filename}"

                    # Verifica se já foi enviado anteriormente
                    with progress_lock:
                        if nifti_filename in progress["uploaded_files"]:
                            log.info(f"⏭️  Pulando {nifti_filename} (já enviado)")
                            continue
                    
                    # Verifica se já existe no destino remoto
                    if check_remote_file_exists(remote_file):
                        log.info(f"☁️  {nifti_filename} já existe no destino")
                        with progress_lock:
                            progress["uploaded_files"].append(nifti_filename)
                            save_progress(progress)
                        continue

                    # CONVERSÃO (apenas se não existe localmente ou remotamente)
                    if not os.path.exists(out):
                        try:
                            convert_series(files, out)
                            log.info(f"✓ Convertido: {nifti_filename}")
                        except Exception as e:
                            log.error(f"❌ Erro na conversão de {nifti_filename}: {e}")
                            raise

                    nii = nib.load(out)
                    meta = {
                        "dicomdir": item,
                        "series_uid": uid,
                        "shape": nii.shape,
                        "spacing": nii.header.get_zooms(),
                        "sha256": sha256(out)
                    }
                    
                    with metadata_lock:
                        metadata_list.append(meta)

                    # UPLOAD
                    try:
                        rclone([
                            "rclone", "copy",
                            out,
                            GDRIVE_NIFTI,
                            "--transfers", "2",
                            "--checkers", "2",
                            "--progress"
                        ])
                        
                        # Registra upload bem-sucedido
                        with progress_lock:
                            progress["uploaded_files"].append(nifti_filename)
                            save_progress(progress)
                        
                        # Remove arquivo local após upload bem-sucedido
                        os.remove(out)
                        
                    except Exception as e:
                        log.error(f"❌ Erro no upload de {nifti_filename}: {e}")
                        # Mantém o arquivo local para tentar novamente depois
                        raise

            shutil.rmtree(path)
            
            with progress_lock:
                progress["converted"].append(item)
                save_progress(progress)
            
            pbar_convert.update(1)
            
        except Exception as e:
            log.error(f"❌ Erro ao processar {item}: {e}")
        
        q_convert.task_done()

# ======================================================
# PIPELINE PRINCIPAL
# ======================================================
def run():
    os.makedirs(LOCAL_DICOM, exist_ok=True)
    os.makedirs(LOCAL_NIFTI, exist_ok=True)

    progress = load_progress()
    progress_lock = threading.Lock()
    metadata_list = []
    metadata_lock = threading.Lock()
    
    # Handler para salvar progresso em caso de interrupção
    def signal_handler(sig, frame):
        log.warning("\n⚠️  Interrupção detectada! Salvando progresso...")
        with progress_lock:
            save_progress(progress)
        log.info("✓ Progresso salvo. Você pode retomar executando o script novamente.")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Recupera arquivos NIfTI locais não enviados
    local_nifti_files = recover_local_nifti_files()
    if local_nifti_files:
        log.info(f"🔄 Encontrados {len(local_nifti_files)} arquivos NIfTI locais para upload")
        for filename in local_nifti_files:
            if filename not in progress.get("uploaded_files", []):
                local_file = os.path.join(LOCAL_NIFTI, filename)
                remote_file = f"{GDRIVE_NIFTI}/{filename}"
                
                if not check_remote_file_exists(remote_file):
                    try:
                        log.info(f"⬆️  Recuperando upload: {filename}")
                        rclone([
                            "rclone", "copy",
                            local_file,
                            GDRIVE_NIFTI,
                            "--transfers", "2",
                            "--checkers", "2"
                        ])
                        progress["uploaded_files"].append(filename)
                        save_progress(progress)
                    except Exception as e:
                        log.error(f"❌ Erro ao recuperar upload de {filename}: {e}")
                else:
                    progress["uploaded_files"].append(filename)
                    save_progress(progress)
                
                # Remove arquivo local após upload
                if filename in progress["uploaded_files"] and os.path.exists(local_file):
                    os.remove(local_file)
    
    log.info("📋 Listando DICOMDIRs remotos...")
    dicomdirs = list_dicomdirs()
    log.info(f"✓ Encontrados {len(dicomdirs)} DICOMDIRs")

    # Filas
    q_download = queue.Queue()
    q_convert = queue.Queue()
    
    # Adiciona apenas os não baixados na fila de download
    to_download = [d for d in dicomdirs if d not in progress["downloaded"]]
    # Adiciona os já baixados mas não convertidos na fila de conversão
    already_downloaded = [d for d in dicomdirs 
                         if d in progress["downloaded"] and d not in progress["converted"]]
    
    for d in to_download:
        q_download.put(d)
    for d in already_downloaded:
        q_convert.put(d)

    total_to_process = len([d for d in dicomdirs if d not in progress["converted"]])

    log.info(f"\n📊 Status: {len(to_download)} para baixar | {len(already_downloaded)} já baixados")
    log.info(f"📊 Uploads já realizados: {len(progress.get('uploaded_files', []))}")
    
    # BARRAS DE PROGRESSO
    with tqdm(total=len(to_download), desc="Downloads", unit="dir", position=0) as pbar_download, \
         tqdm(total=total_to_process, desc="Conversões", unit="dir", position=1) as pbar_convert:
        
        # Inicia workers de download
        download_threads = []
        for _ in range(DOWNLOAD_THREADS):
            t = threading.Thread(
                target=download_worker,
                args=(q_download, q_convert, progress, pbar_download, progress_lock),
                daemon=True
            )
            t.start()
            download_threads.append(t)
        
        # Inicia workers de conversão
        conversion_threads = []
        for _ in range(CONVERSION_THREADS):
            t = threading.Thread(
                target=conversion_worker,
                args=(q_convert, progress, metadata_list, pbar_convert, progress_lock, metadata_lock),
                daemon=True
            )
            t.start()
            conversion_threads.append(t)
        
        # Aguarda downloads terminarem
        q_download.join()
        
        # Finaliza workers de download
        for _ in range(DOWNLOAD_THREADS):
            q_download.put(None)
        for t in download_threads:
            t.join()
        
        # Aguarda conversões terminarem
        q_convert.join()
        
        # Finaliza workers de conversão
        for _ in range(CONVERSION_THREADS):
            q_convert.put(None)
        for t in conversion_threads:
            t.join()

    # METADATA
    log.info("\n💾 Salvando metadados...")
    if metadata_list:
        pd.DataFrame(metadata_list).to_csv(
            os.path.join(WORKDIR, "metadata.csv"),
            index=False
        )
        log.info(f"✓ {len(metadata_list)} séries processadas")
    else:
        log.info("⚠️  Nenhuma série nova para processar")

    log.info("\n🏁 PIPELINE FINALIZADO COM SUCESSO!")

# ======================================================
if __name__ == "__main__":
    run()

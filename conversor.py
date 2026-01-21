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
import argparse
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
DICOMDIRS_CACHE = os.path.join(WORKDIR, "dicomdirs_cache.json")

MIN_SLICES = CONFIG["processing"]["min_slices"]
DOWNLOAD_THREADS = CONFIG["processing"]["download_threads"]
CONVERSION_THREADS = CONFIG["processing"]["conversion_threads"]

# ======================================================
# LOG
# ======================================================
logging.basicConfig(
    level=logging.DEBUG,  # Mudado para DEBUG temporariamente
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("PIPELINE")
log.setLevel(logging.DEBUG)

# Wrapper para logs compatíveis com tqdm
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
        except Exception:
            self.handleError(record)

# Adiciona handler tqdm ao logger
tqdm_handler = TqdmLoggingHandler()
tqdm_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"))
log.addHandler(tqdm_handler)
log.propagate = False

# ======================================================
# UTIL
# ======================================================
# Lista global para rastrear processos ativos
active_processes = []
active_processes_lock = threading.Lock()

def load_progress():
    """Carrega progresso de arquivo JSON com tratamento de erros"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                data = json.load(f)
                
                # Migração/validação: adiciona campos se não existirem
                if "uploaded_files" not in data:
                    data["uploaded_files"] = []
                if "downloaded" not in data:
                    data["downloaded"] = []
                if "converted" not in data:
                    data["converted"] = []
                if "uploaded" not in data:
                    data["uploaded"] = []
                
                log.info(f"📂 Progresso carregado: {len(data['downloaded'])} baixados, {len(data['converted'])} convertidos, {len(data['uploaded_files'])} enviados")
                return data
        except json.JSONDecodeError as e:
            log.error(f"❌ Erro ao ler progress.json (arquivo corrompido): {e}")
            # Tenta backup
            backup_file = PROGRESS_FILE + ".backup"
            try:
                shutil.copy2(PROGRESS_FILE, backup_file)
                log.warning(f"⚠️  Backup criado em {backup_file}")
            except Exception:
                pass
        except Exception as e:
            log.error(f"❌ Erro ao carregar progresso: {e}")
    
    log.info("🆕 Criando novo arquivo de progresso")
    return {
        "downloaded": [],
        "converted": [],
        "uploaded": [],
        "uploaded_files": []  # Lista de arquivos NIfTI já enviados
    }

def save_progress(p):
    """Salva progresso em arquivo JSON com tratamento de erros"""
    try:
        # Garante que o diretório existe
        os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
        
        # Salva em arquivo temporário primeiro
        temp_file = PROGRESS_FILE + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(p, f, indent=2)
        
        # Substitui o arquivo original (operação atômica no Windows)
        if os.path.exists(PROGRESS_FILE):
            os.replace(temp_file, PROGRESS_FILE)
        else:
            os.rename(temp_file, PROGRESS_FILE)
            
        log.debug(f"Progresso salvo: {len(p.get('downloaded', []))} baixados, {len(p.get('converted', []))} convertidos, {len(p.get('uploaded_files', []))} enviados")
    except Exception as e:
        log.error(f"❌ Erro ao salvar progresso: {e}")
        # Tenta salvar diretamente como fallback
        try:
            with open(PROGRESS_FILE, "w") as f:
                json.dump(p, f, indent=2)
        except Exception as e2:
            log.error(f"❌ Erro crítico ao salvar progresso: {e2}")

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(8192), b""):
            h.update(b)
    return h.hexdigest()

def rclone(cmd, show_progress=False):
    """Executa comando rclone com registro para permitir terminação controlada"""
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE if not show_progress else None,
        stderr=subprocess.PIPE if not show_progress else None,
        text=True
    )
    
    # Registra processo ativo
    with active_processes_lock:
        active_processes.append(process)
    
    try:
        # Aguarda conclusão
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd, stdout, stderr)
        
        return stdout
    finally:
        # Remove da lista de processos ativos
        with active_processes_lock:
            if process in active_processes:
                active_processes.remove(process)

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
def load_dicomdirs_cache():
    """Carrega cache de dicomdirs previamente listados"""
    if os.path.exists(DICOMDIRS_CACHE):
        try:
            with open(DICOMDIRS_CACHE, 'r') as f:
                data = json.load(f)
                return data.get("dicomdirs", []), data.get("complete", False)
        except Exception as e:
            log.warning(f"⚠️  Erro ao ler cache de dicomdirs: {e}")
    return [], False

def save_dicomdirs_cache(dicomdirs, complete=False):
    """Salva cache de dicomdirs"""
    try:
        with open(DICOMDIRS_CACHE, 'w') as f:
            json.dump({
                "dicomdirs": dicomdirs,
                "complete": complete,
                "timestamp": time.time()
            }, f, indent=2)
    except Exception as e:
        log.error(f"❌ Erro ao salvar cache de dicomdirs: {e}")

def list_dicomdirs(force_refresh=False):
    """Lista dicomdirs remotos com cache e retomada"""
    # Verifica cache existente
    cached_dirs, is_complete = load_dicomdirs_cache()
    
    # Se forçar refresh, ignora cache
    if force_refresh:
        log.info("🔄 Forçando atualização do cache de DICOMDIRs")
        cached_dirs = []
        is_complete = False
    elif is_complete and cached_dirs:
        log.info(f"✓ Cache encontrado com {len(cached_dirs)} DICOMDIRs")
        return cached_dirs
    
    # Se cache incompleto, continua de onde parou
    if cached_dirs:
        log.info(f"🔄 Retomando listagem (cache parcial: {len(cached_dirs)} DICOMDIRs)")
    
    log.info("📋 Listando DICOMDIRs remotos...")
    
    try:
        # Lista todos os diretórios recursivamente
        cmd = ["rclone", "lsf", "--dirs-only", "--recursive", GDRIVE_DICOM]
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        dicomdirs = []
        seen = set(cached_dirs)  # Para evitar duplicatas ao retomar
        
        # Processa linha por linha
        for line in process.stdout:
            dicomdir = line.strip().rstrip("/")
            if dicomdir and dicomdir not in seen:
                dicomdirs.append(dicomdir)
                seen.add(dicomdir)
                
                # Mostra progresso sobrescrevendo a linha
                print(f"\r  📂 {dicomdir[:70]:<70}", end="", flush=True)
                
                # Salva cache periodicamente (a cada 10 itens)
                if len(dicomdirs) % 10 == 0:
                    all_dirs = cached_dirs + dicomdirs
                    save_dicomdirs_cache(all_dirs, complete=False)
        
        print()  # Nova linha após terminar
        
        # Aguarda processo terminar
        process.wait()
        
        if process.returncode == 0:
            # Combina cache antigo com novos
            all_dirs = cached_dirs + dicomdirs
            save_dicomdirs_cache(all_dirs, complete=True)
            log.info(f"✓ Listagem completa: {len(all_dirs)} DICOMDIRs encontrados")
            return all_dirs
        else:
            # Erro, mas salva o que conseguiu
            all_dirs = cached_dirs + dicomdirs
            save_dicomdirs_cache(all_dirs, complete=False)
            stderr = process.stderr.read()
            raise Exception(f"rclone retornou erro: {stderr}")
            
    except KeyboardInterrupt:
        # Interrompido pelo usuário, salva progresso
        all_dirs = cached_dirs + dicomdirs
        save_dicomdirs_cache(all_dirs, complete=False)
        log.warning(f"\n⚠️  Listagem interrompida. {len(all_dirs)} DICOMDIRs salvos no cache.")
        raise
    except Exception as e:
        # Salva o que conseguiu antes de propagar erro
        if 'dicomdirs' in locals():
            all_dirs = cached_dirs + dicomdirs
            save_dicomdirs_cache(all_dirs, complete=False)
        log.error(f"❌ Erro na listagem: {e}")
        raise

# ======================================================
# DOWNLOAD
# ======================================================
def download_worker(q_download, q_convert, progress, pbar_download, progress_lock):
    worker_id = threading.current_thread().name
    log.debug(f"[{worker_id}] Worker de download iniciado")
    
    try:
        while True:
            try:
                item = q_download.get(timeout=1)  # Timeout para não travar
            except queue.Empty:
                continue
            
            if item is None:
                log.debug(f"[{worker_id}] Recebeu sinal de encerramento")
                break

            local = os.path.join(LOCAL_DICOM, item)
            
            # Verifica se o diretório local existe e tem conteúdo
            local_exists = os.path.exists(local) and os.path.isdir(local) and len(os.listdir(local)) > 0
            
            with progress_lock:
                already_registered = item in progress["downloaded"]
            
            # Se já está registrado como baixado E o diretório existe localmente
            if already_registered and local_exists:
                pbar_download.update(1)
                q_convert.put(item)
                q_download.task_done()
                continue

            os.makedirs(local, exist_ok=True)

            try:
                pbar_download.set_description(f"⬇️  {item[:40]}")
                
                rclone([
                    "rclone", "copy",
                    f"{GDRIVE_DICOM}/{item}",
                    local,
                    "--transfers", "2",
                    "--checkers", "2"
                ])
                
                # Verifica se o download foi bem-sucedido
                if os.path.exists(local) and len(os.listdir(local)) > 0:
                    file_count = len(os.listdir(local))
                    
                    with progress_lock:
                        if item not in progress["downloaded"]:
                            progress["downloaded"].append(item)
                        save_progress(progress)
                    
                    pbar_download.set_postfix_str(f"{file_count} arquivos")
                    pbar_download.update(1)
                    
                    # Envia para fila de conversão
                    q_convert.put(item)
                else:
                    log.error(f"❌ Download falhou (diretório vazio): {item}")
                
            except KeyboardInterrupt:
                raise  # Propaga para encerrar a thread
            except Exception as e:
                log.error(f"❌ Erro no download de {item}: {e}")
                # Não marca como baixado para tentar novamente
            
            q_download.task_done()
            
    except KeyboardInterrupt:
        log.debug(f"[{worker_id}] Worker de download recebeu interrupção")
        return
    
    log.debug(f"[{worker_id}] Worker de download encerrado")

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
    try:
        while True:
            item = q_convert.get()
            if item is None:
                break

            with progress_lock:
                if item in progress["converted"]:
                    log.info(f"⏭️  {item} já convertido")
                    pbar_convert.update(1)
                    q_convert.task_done()
                    continue

            path = os.path.join(LOCAL_DICOM, item)
            
            # Verifica se o diretório existe
            if not os.path.exists(path):
                log.error(f"❌ Diretório não encontrado: {path}")
                q_convert.task_done()
                continue
            
            conversion_successful = True
            
            try:
                pbar_convert.set_description(f"� {item[:25]}")
                
                series = parse_dicomdir(path)
                
                # Filtrar séries válidas
                valid_series = {uid: files for uid, files in series.items() 
                              if len(files) >= MIN_SLICES}
                
                if not valid_series:
                    log.warning(f"⚠️  {item} não possui séries válidas (min {MIN_SLICES} slices)")
                
                series_processed = 0
                total_series = len(valid_series)
                
                for idx, (uid, files) in enumerate(valid_series.items(), 1):
                    pbar_convert.set_description(f"🧠 {item[:20]} [{idx}/{total_series}]")
                    nifti_filename = f"{item}_{uid}.nii.gz"
                    out = os.path.join(LOCAL_NIFTI, nifti_filename)
                    remote_file = f"{GDRIVE_NIFTI}/{nifti_filename}"

                    # Verifica se já foi enviado anteriormente
                    with progress_lock:
                        if nifti_filename in progress["uploaded_files"]:
                            log.info(f"⏭️  {nifti_filename} já enviado")
                            series_processed += 1
                            continue
                    
                    # Verifica se já existe no destino remoto
                    if check_remote_file_exists(remote_file):
                        log.info(f"☁️  {nifti_filename} já existe no destino")
                        with progress_lock:
                            if nifti_filename not in progress["uploaded_files"]:
                                progress["uploaded_files"].append(nifti_filename)
                            save_progress(progress)
                        series_processed += 1
                        continue

                    # CONVERSÃO (apenas se não existe localmente)
                    if not os.path.exists(out):
                        try:
                            convert_series(files, out)
                        except Exception as e:
                            log.error(f"❌ Erro na conversão de {nifti_filename}: {e}")
                            conversion_successful = False
                            break  # Para de processar séries deste DICOMDIR

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
                    pbar_convert.set_description(f"⬆️  {item[:20]} [{idx}/{total_series}]")
                    try:
                        rclone([
                            "rclone", "copy",
                            out,
                            GDRIVE_NIFTI,
                            "--transfers", "2",
                            "--checkers", "2"
                        ])
                        
                        # Registra upload bem-sucedido
                        with progress_lock:
                            if nifti_filename not in progress["uploaded_files"]:
                                progress["uploaded_files"].append(nifti_filename)
                            save_progress(progress)
                        
                        # Remove arquivo local após upload bem-sucedido
                        os.remove(out)
                        series_processed += 1
                        
                    except Exception as e:
                        log.error(f"❌ Erro no upload de {nifti_filename}: {e}")
                        conversion_successful = False
                        break  # Para de processar séries deste DICOMDIR

                # Só remove o diretório e marca como convertido se tudo foi processado
                if conversion_successful and series_processed == len(valid_series):
                    try:
                        shutil.rmtree(path)
                        log.info(f"🗑️  Removido diretório local: {item}")
                    except Exception as e:
                        log.warning(f"⚠️  Erro ao remover diretório {path}: {e}")
                    
                    with progress_lock:
                        if item not in progress["converted"]:
                            progress["converted"].append(item)
                        save_progress(progress)
                    
                    log.info(f"✓ {item} processado completamente ({series_processed} séries)")
                    pbar_convert.update(1)
                else:
                    log.warning(f"⚠️  {item} processado parcialmente - será reprocessado depois")
                
            except KeyboardInterrupt:
                raise  # Propaga para encerrar a thread
            except Exception as e:
                log.error(f"❌ Erro ao processar {item}: {e}")
            
            q_convert.task_done()
    except KeyboardInterrupt:
        log.debug("Worker de conversão recebeu interrupção")
        return

# ======================================================
# PIPELINE PRINCIPAL
# ======================================================
def run(force_refresh=False):
    os.makedirs(LOCAL_DICOM, exist_ok=True)
    os.makedirs(LOCAL_NIFTI, exist_ok=True)
    os.makedirs(WORKDIR, exist_ok=True)  # Garante que workdir existe

    progress = load_progress()
    
    # Testa gravação do progress imediatamente
    try:
        save_progress(progress)
        log.info("✓ Teste de gravação do progress.json bem-sucedido")
    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: Não é possível gravar progress.json: {e}")
        log.error(f"   Caminho: {PROGRESS_FILE}")
        log.error(f"   Verifique permissões de escrita no diretório")
        return
    
    progress_lock = threading.Lock()
    metadata_list = []
    metadata_lock = threading.Lock()
    
    # Flag para prevenir múltiplas execuções do handler
    shutdown_in_progress = {"flag": False, "count": 0}
    
    # Handler para salvar progresso em caso de interrupção
    def signal_handler(sig, frame):
        shutdown_in_progress["count"] += 1
        
        # Se já pressionou Ctrl+C mais de 2 vezes, força encerramento imediato
        if shutdown_in_progress["count"] > 2:
            log.error("\n🔴 FORÇANDO ENCERRAMENTO IMEDIATO!")
            os._exit(1)
        
        # Previne execução múltipla do handler principal
        if shutdown_in_progress["flag"]:
            log.warning(f"\n⚠️  Pressione Ctrl+C mais {3 - shutdown_in_progress['count']} vezes para forçar encerramento")
            return
        shutdown_in_progress["flag"] = True
        
        log.warning("\n⚠️  Interrupção detectada! Encerrando processos...")
        log.info("⏳ Aguarde o encerramento gracioso... (pressione Ctrl+C 2x mais para forçar)")
        
        # Termina todos os processos rclone ativos
        with active_processes_lock:
            if active_processes:
                log.info(f"🛑 Terminando {len(active_processes)} processos rclone...")
                for proc in active_processes[:]:  # Copia a lista para iterar
                    try:
                        proc.terminate()
                    except Exception as e:
                        log.debug(f"Erro ao terminar processo: {e}")
                
                # Aguarda um pouco para terminação graciosamente
                time.sleep(1)
                
                # Força kill se ainda estiverem rodando
                for proc in active_processes[:]:
                    try:
                        if proc.poll() is None:  # Ainda está rodando
                            proc.kill()
                            log.debug("Processo forçado a encerrar")
                    except Exception as e:
                        log.debug(f"Erro ao forçar encerramento: {e}")
        
        log.info("💾 Salvando progresso...")
        # Faz cópia thread-safe do progresso antes de salvar
        try:
            with progress_lock:
                progress_snapshot = {
                    "downloaded": progress["downloaded"][:],
                    "converted": progress["converted"][:],
                    "uploaded": progress["uploaded"][:],
                    "uploaded_files": progress["uploaded_files"][:]
                }
            
            save_progress(progress_snapshot)
            log.info("✓ Progresso salvo. Você pode retomar executando o script novamente.")
            log.info(f"   • {len(progress_snapshot['downloaded'])} downloads salvos")
            log.info(f"   • {len(progress_snapshot['converted'])} conversões salvas")
            log.info(f"   • {len(progress_snapshot['uploaded_files'])} uploads salvos")
        except Exception as e:
            log.error(f"❌ Erro ao salvar progresso: {e}")
        
        # Usa os._exit() para forçar encerramento imediato
        log.info("👋 Encerrando...")
        os._exit(0)
    
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
    
    dicomdirs = list_dicomdirs(force_refresh=force_refresh)

    # Filas
    q_download = queue.Queue()
    q_convert = queue.Queue()
    
    # Separa DICOMDIRs por status
    to_download = []
    to_convert = []
    completed = []
    
    for d in dicomdirs:
        if d in progress["converted"]:
            completed.append(d)
        elif d in progress["downloaded"]:
            # Verifica se o diretório local ainda existe
            local_path = os.path.join(LOCAL_DICOM, d)
            if os.path.exists(local_path) and os.path.isdir(local_path):
                to_convert.append(d)
            else:
                # Registrado como baixado mas não existe localmente - rebaixar
                log.warning(f"⚠️  {d} registrado como baixado mas não encontrado localmente")
                to_download.append(d)
        else:
            to_download.append(d)
    
    # Adiciona nas filas
    for d in to_download:
        q_download.put(d)
    for d in to_convert:
        q_convert.put(d)

    total_to_process = len(to_download) + len(to_convert)

    log.info(f"\n📊 Status:")
    log.info(f"   • {len(to_download)} para baixar")
    log.info(f"   • {len(to_convert)} para converter")
    log.info(f"   • {len(completed)} já processados")
    log.info(f"   • {len(progress.get('uploaded_files', []))} arquivos NIfTI enviados")
    
    if total_to_process == 0:
        log.info("\n✓ Todos os DICOMDIRs já foram processados!")
        # Salva metadados mesmo se não houver novos
        log.info("\n💾 Salvando metadados...")
        if os.path.exists(os.path.join(WORKDIR, "metadata.csv")):
            log.info("✓ Arquivo metadata.csv já existe")
        else:
            log.info("⚠️  Nenhum metadado para salvar")
        return
    
    # BARRAS DE PROGRESSO
    log.info(f"\n🚀 Iniciando processamento com {DOWNLOAD_THREADS} threads de download e {CONVERSION_THREADS} de conversão")
    
    with tqdm(total=len(to_download), desc="Downloads", unit="dir", position=0, leave=True, dynamic_ncols=True) as pbar_download, \
         tqdm(total=total_to_process, desc="Conversões", unit="dir", position=1, leave=True, dynamic_ncols=True) as pbar_convert:
        
        # Inicia workers de download
        download_threads = []
        for i in range(DOWNLOAD_THREADS):
            t = threading.Thread(
                target=download_worker,
                args=(q_download, q_convert, progress, pbar_download, progress_lock),
                daemon=False,  # Não daemon para permitir encerramento controlado
                name=f"Download-{i+1}"
            )
            t.start()
            download_threads.append(t)
        
        log.info(f"✓ {len(download_threads)} threads de download iniciadas")
        
        # Inicia workers de conversão
        conversion_threads = []
        for i in range(CONVERSION_THREADS):
            t = threading.Thread(
                target=conversion_worker,
                args=(q_convert, progress, metadata_list, pbar_convert, progress_lock, metadata_lock),
                daemon=False,  # Não daemon para permitir encerramento controlado
                name=f"Conversion-{i+1}"
            )
            t.start()
            conversion_threads.append(t)
        
        log.info(f"✓ {len(conversion_threads)} threads de conversão iniciadas")
        log.info(f"📊 Itens na fila de download: {q_download.qsize()}")
        log.info(f"📊 Itens na fila de conversão: {q_convert.qsize()}")
        
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
    parser = argparse.ArgumentParser(
        description="Pipeline de conversão DICOM para NIfTI com upload para Google Drive"
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Força atualização do cache de DICOMDIRs, ignorando cache existente"
    )
    
    args = parser.parse_args()
    run(force_refresh=args.refresh_cache)

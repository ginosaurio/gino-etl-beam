import os
import shutil
import zipfile
import tempfile
import requests

# URL del ZIP del repo (rama main) de TU repositorio
REPO_ZIP_URL = "https://github.com/ginosaurio/gino-etl-beam/archive/refs/heads/main.zip"

# Carpeta donde queremos dejar los archivos de datos para el pipeline
LOCAL_DATA_DIR = "./data"


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def download_repo_zip(url: str, dest_path: str) -> None:
    print(f"Descargando repo desde: {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(resp.content)
    print(f"ZIP guardado en: {dest_path}")


def extract_zip(zip_path: str, extract_to: str) -> str:
    """
    Extrae el ZIP en extract_to y devuelve la ruta a la carpeta raíz del repo.
    GitHub crea algo como gino-etl-beam-main/ dentro del ZIP.
    """
    print(f"Extrayendo ZIP en: {extract_to}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)

    # Buscar la carpeta raíz (la primera carpeta que aparezca)
    entries = [
        name for name in os.listdir(extract_to)
        if os.path.isdir(os.path.join(extract_to, name))
    ]
    if not entries:
        raise RuntimeError("No se encontró carpeta raíz del repo en el ZIP.")
    root_dir = os.path.join(extract_to, entries[0])
    print(f"Carpeta raíz del repo: {root_dir}")
    return root_dir


def copy_data_folder(repo_root: str, local_data_dir: str) -> None:
    """
    Copia la carpeta data/ del repo extraído a ./data (sobrescribiendo archivos).
    """
    source_data_dir = os.path.join(repo_root, "data")
    if not os.path.isdir(source_data_dir):
        raise RuntimeError(f"No se encontró carpeta 'data' en {repo_root}")

    ensure_dir(local_data_dir)

    for name in os.listdir(source_data_dir):
        src = os.path.join(source_data_dir, name)
        dst = os.path.join(local_data_dir, name)
        if os.path.isfile(src):
            print(f"Copiando {src} -> {dst}")
            shutil.copy2(src, dst)


def main():
    ensure_dir(LOCAL_DATA_DIR)

    # Carpeta temporal para ZIP + extracción
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "repo.zip")

        # 1. Descargar ZIP del repo
        download_repo_zip(REPO_ZIP_URL, zip_path)

        # 2. Extraer ZIP
        repo_root = extract_zip(zip_path, tmpdir)

        # 3. Copiar data/ -> ./data
        copy_data_folder(repo_root, LOCAL_DATA_DIR)

    print("Descarga y copia de datos completada.")
    print(f"Archivos de datos disponibles en: {os.path.abspath(LOCAL_DATA_DIR)}")


if __name__ == "__main__":
    main()

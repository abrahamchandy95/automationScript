from collections import Counter
import json
import math
import os
from pathlib import Path
import shutil
from typing import Optional, List, Dict, Tuple, Union
import zipfile
import pandas as pd
from pdf2docx import Converter
import rarfile

# Directory Functions
def assert_exists(path: Optional[Path], err_msg):
    if path is None:
        raise FileNotFoundError(f"{err_msg}")

def create_dir(path: Union[str, Path]) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path

def find_dir(path: Union[str, Path], patterns: List[str]) -> Path:
    path = Path(path)
    for d in path.iterdir():
        if d.is_dir() and any(p.lower() in d.name.lower() for p in patterns):
            return d
    raise FileNotFoundError(f"No directory found in {path} matching {patterns}")

# File Functions
def copy_file(
    src: Union[str, Path], trg: Union[str, Path], renamed: Optional[str]=None
) -> None:
    src, trg = Path(src), Path(trg)
    trg_path = trg/(renamed if renamed else src.name)
    shutil.copy2(src, trg_path)

def copy_dir(
    src: Union[str, Path], trg: Union[str, Path], renamed: Optional[str]=None
) -> None:
    src, trg = Path(src), Path(trg)
    trg = trg/(renamed if renamed else src.name)

    try:
        if trg.exists():
            shutil.rmtree(trg, ignore_errors=True)
        shutil.copytree(src, trg)
    except Exception as e:
        print(f"Error copying directory: {e}")

def unzip_file(src: Union[str, Path], trg: Union[str, Path]) -> None:
    src, trg = Path(src), Path(trg)
    if src.suffix == '.zip':
        with zipfile.ZipFile(src, 'r') as zip_ref:
            zip_ref.extractall(trg)
        src.unlink()
    elif src.suffix == '.rar':
        with rarfile.RarFile(src, 'r') as rar_ref:
            rar_ref.extractall(trg)
            src.unlink()

def unzip_leaves(trg_dir: Union[str, Path]) -> None:
    trg_dir = Path(trg_dir)
    for c in trg_dir.iterdir():
        if c.is_file():
            unzip_file(c, trg_dir)
        elif c.is_dir():
            unzip_leaves(c)

def flatten_dir(root: Path):
    """ Moves all leaves of a directory to the parent directory. """
    root = Path(root)
    if root.is_file():
        if root.suffix in ['zip', 'rar']:
            unzip_file(root, root.parent)
        return

    for subdir, _, files in os.walk(root, topdown=False):
        subdir = Path(subdir)
        for file in files:

            src = subdir/file
            trg = root/file

            if src.suffix in ['rar', 'zip']:
                unzip_leaves(src)

            if src != trg:
                shutil.move(src, trg)
        # Remove empty subdirectories
        if not any(subdir.iterdir()):
            subdir.rmdir()


def pdf_to_docx(pdf_path: Path, output_dir: Path, docx_filename: str) -> None:

    create_dir(output_dir)

    docx_out_path = output_dir / docx_filename
    cv = Converter(str(pdf_path))
    cv.convert(str(docx_out_path), start=0)
    cv.close()

# String functions
def is_match_with_pids(
        char_part: str, point_ids: List[str]
) -> bool:
    """
    Checks if a given string matches with any of the POINT IDS
    """
    char_counts = Counter(char_part.lower())
    for pid in point_ids:
        counts = Counter(pid.lower())
        if all(char_counts[c] <= counts.get(c, 0) for c in char_counts):
            return True
    return False

def rename_file_per(f: str, rename_rules: Dict[str, str])-> str:
    for name, rule in rename_rules.items():
        if name.lower() in f.lower():
            return rule.format(filename=f)
    return f

# load from config.py
def load_config(config_file: str='config.json') -> Dict:
    with open(config_file, 'r') as f:
        return json.load(f)

def config_val(config: Dict, key: str) -> Optional[str]:
    return config.get(key, None)

def load_csv(
        csv_path: Path, delimiter: Optional[str] = None, skip: int = 0
    ) -> pd.DataFrame:
        if delimiter:
            df = pd.read_csv(
                csv_path, delimiter=delimiter, skiprows=skip, header=None
            )
        else:
            df = pd.read_csv(csv_path, skiprows=skip, header=None)
        df.dropna(axis=1, how='all', inplace=True)

        return df

def calculate_euclidean(
        X1: Tuple[float, float, float],
        X2: Tuple[float, float, float]
)-> Optional[float]:
    """
    Calculates the Euclidean distance between two 3D points
    """
    if None in X1 or None in X2:
        return None
    d = math.sqrt(
        (X1[0]-X2[0])**2 + (X1[1]-X2[1])**2 + (X1[2]-X2[2])**2
    )
    return d

"""Functions to transform/visualize GoT data."""
import warnings
from os.path import join
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import pyvista as pv
from scipy.signal import medfilt
from scipy.spatial.distance import cdist


def get_df(
    files: List[str], folder: Optional[str] = None, keep_unique: bool = True
) -> pd.DataFrame:
    """Transform raw GoT Output files into one Dataframe.

    Args:
        files (List[str]): List of filenames
        folder (Optional[str], optional): Folder containing all the files. Example: './data/'.
                                          (None equals to the current folder).
        keep_unique (bool): If True keep all columns with only one same value.

    Returns:
        pd.DataFrame: Dataframe with named columns if known.
    """
    frames = []
    for file in files:
        if folder is not None:
            file = join(folder, file)
        temp = pd.read_csv(file, sep="[,|;]", engine="python", header=None)
        frames.append(temp)

    df = pd.concat(frames, ignore_index=True, axis=0)
    df.drop(df.columns[-1], axis=1, inplace=True)
    df.rename(columns={df.columns[0]: "pc_timestamp"}, inplace=True)
    df.rename(columns={df.columns[1]: "time_after_start_ms"}, inplace=True)
    df.rename(columns={df.columns[2]: "transmitter_id"}, inplace=True)
    df.rename(columns={df.columns[3]: "valid_flg"}, inplace=True)
    df.rename(columns={df.columns[4]: "x"}, inplace=True)
    df.rename(columns={df.columns[5]: "y"}, inplace=True)
    df.rename(columns={df.columns[6]: "z"}, inplace=True)
    for i in range(7, df.shape[1] - 1, 3):
        k = int((i - 6) / 3)
        df.rename(columns={df.columns[i]: f"receiver_id_{k}"}, inplace=True)
        df.rename(columns={df.columns[i + 1]: f"distance_{k}"}, inplace=True)
        df.rename(columns={df.columns[i + 2]: f"level_{k}"}, inplace=True)

    if not keep_unique:
        for col in df.columns:
            if np.unique(df[[col]]).shape[0] == 1:
                df.drop(columns=[col], inplace=True)

    return df

def get_cloud(df: pd.DataFrame) -> pv.PolyData:
    """Get PolyData cloud from Dataframe and do sanity check.

    Args:
        df (pd.DataFrame): Dataframe containing x, y, z columns.

    Returns:
        pv.PolyData: pyvista cloud.
    """
    points = df[["x", "y", "z"]].to_numpy(dtype=np.float32)
    points = pv.pyvista_ndarray(points)
    cloud = pv.PolyData(points)
    np.allclose(points, cloud.points)
    return cloud
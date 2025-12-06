"""Create a time-indexed dataset from historic PMED CSV data.

This module provides a helper `create_dataset` that pivots raw CSV rows
into a time-indexed table (timestamps x sensor IDs) and saves it to an
HDF store. It also exposes a Hydra `main` function for CLI execution.
"""

import os
import logging
import pandas as pd
import hydra
from omegaconf import DictConfig

log = logging.getLogger(__name__)


def create_dataset(path, dst_path, column="vmed"):
    """Read a CSV and pivot it into a time-indexed table.

    Parameters
    ----------
    path : str
        Path to the input CSV file containing PMED measurements.
    dst_path : str
        Destination path for the HDF output.
    column : str, optional
        Column name to use as the values in the pivot (default: "vmed").
    """
    log.info("Reading dataframe...")
    df = pd.read_csv(path, delimiter=";", encoding="latin1")
    df["fecha"] = pd.to_datetime(df["fecha"])
    #log.info(f"Dataset shape: {df.shape}")
    log.info("Pivoting...")
    table = df.pivot(index="fecha", columns="id", values=column).fillna(0.0)
    log.info("Saving...")
    table.to_hdf(dst_path, column)


@hydra.main(config_path="conf", config_name="dataset")
def main(cfg: DictConfig):
    """Hydra entrypoint to create the dataset.

    Parameters
    ----------
    cfg : omegaconf.DictConfig
        Configuration read by Hydra. Expected keys include `results`,
        `historic_name`, `dataset_name`, and `dataset_parameter`.
    """
    log.info("="*80)
    log.info(" " * 20 + "Create Dataset")
    log.info("="*80)

    sourcepath = os.path.join(cfg.results, cfg.historic_name)
    filepath = os.path.join(cfg.results, cfg.dataset_name)
    log.info(f"Creating dataset from: {sourcepath}")
    log.info(f"Dataset output folder: {filepath}")

    if not os.path.exists(sourcepath):
        raise FileNotFoundError(f"File not found: {sourcepath}")

    create_dataset(sourcepath, filepath, column=cfg.dataset_parameter)

    log.info("Dataset created!")


if __name__ == "__main__":
    main()

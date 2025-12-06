"""Extract PMED measurements from historic monthly CSV files.

This script reads configuration from Hydra (`conf/dataset.yaml`) and
filters historic monthly CSV files to only keep rows corresponding to a
preselected list of PMED sensor IDs. The filtered result is concatenated
and written to an output CSV.

Typical usage:
    python -m dataset.extract_pmed_from_historic
"""

import os
import logging
from glob import glob
import pandas as pd
import hydra
from rich.progress import track
from omegaconf import DictConfig

log = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="dataset")
def main(cfg: DictConfig) -> None:
    """Hydra entrypoint to extract PMEDs from historic CSV files.

    Parameters
    ----------
    cfg : omegaconf.DictConfig
        Configuration read by Hydra. Expected keys include `results`,
        `pmed_list_name`, `historic_path` and `historic_name`.
    """
    log.info("="*80)
    log.info(" " * 20 + "Extract pmed from database")
    log.info("="*80)

    # Load selected pmed list
    log.info("Load selected pmed list")

    with open(os.path.join(cfg.results, cfg.pmed_list_name), "r") as f:
        pmeds = [line.replace("\n", "") for line in f.readlines()]

    historic_csv = sorted(glob(os.path.join(cfg.historic_path, "*.csv")))
    cols = ["id", "fecha", "vmed", "intensidad", "ocupacion", "carga"]
    m30 = []
    for month in track(historic_csv, description="Reading historic data..."):
        log.info(f"Loading file: {month}")
        df = pd.read_csv(month, sep=";", encoding="latin1")
        m30.append(pd.concat([df[df["id"] == int(pmed)][cols]
                              for pmed in pmeds], ignore_index=True))

    # Concatenate dataframes for all months.
    # Replace all missing values (NaN) with 0.0
    m30 = pd.concat(m30, ignore_index=True).fillna(0.0)
    log.info("Save filtered dataframe")
    m30.to_csv(os.path.join(cfg.results, cfg.historic_name),
               sep=";", index=False)

    log.info("Extraction finished!")


if __name__ == "__main__":
    main()

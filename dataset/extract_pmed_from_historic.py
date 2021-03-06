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
    """
    Extract PMEDs from historic data.

    Parameters
    ----------
    cfg : dict
        Configuration dictionary.  
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

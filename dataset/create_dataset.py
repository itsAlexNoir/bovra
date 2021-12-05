import os
import logging
import pandas as pd
import hydra
from omegaconf import DictConfig

log = logging.getLogger(__name__)


def create_dataset(path, dst_path, column="vmed"):
    """
    Create a dataset from a csv file.
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
    """
    Main function.
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

import os
import logging
import pandas as pd
import numpy as np
import hydra
from omegaconf import DictConfig
from rich.progress import track

log = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="dataset")
def main(cfg: DictConfig) -> None:

    log.info("=" * 80)
    log.info(" " * 25 + "Getting sensor info")
    log.info("=" * 80)

    filepath = os.path.join(cfg.results, cfg.dataset_name)
    historic = pd.read_hdf(filepath)
    valid_pmed = historic.columns.tolist()

    num_pmed = len(valid_pmed)
    log.info(f"Number of pmed to be considered: {num_pmed}")
    tofile = [str(pts)+"," for pts in valid_pmed]
    tofile[-1] = tofile[-1].replace(",", " ")
    # Save selected pmed's
    with open(os.path.join(cfg.results, cfg.sensor_id_file), 'w') as f:
        [f.write(pts) for pts in tofile]

    locations_df = pd.read_csv(os.path.join(
        cfg.results, cfg.pmed_locations_name))

    # Filter by valid pmed
    filtered = pd.concat([locations_df[locations_df["id"] == pmed].iloc[-1:]
                         for pmed in valid_pmed], axis=0, ignore_index=True)

    filtered.to_csv(os.path.join(
        cfg.results, cfg.sensor_locations_file), index=False)

    # Calculate square matrix of distances
    log.info("Calculate square matrix distances...")
    to = []
    fromm = []
    cost = []

    for idx in track(filtered.index, description="Calculating distances"):
        for jdx in filtered.index:
            to.append(filtered.loc[idx, "id"])
            fromm.append(filtered.loc[jdx, "id"])
            cost.append(np.sqrt((filtered.loc[idx, "utm_x"] - filtered.loc[jdx, "utm_x"])**2 +
                                (filtered.loc[idx, "utm_y"] - filtered.loc[jdx, "utm_y"])**2))

    pd.DataFrame(data={"to": to, "from": fromm, "cost": cost}).to_csv(
        os.path.join(cfg.results, cfg.sensor_distances_file), index=False)

    log.info("Sensor script finished!")


if __name__ == "__main__":
    main()

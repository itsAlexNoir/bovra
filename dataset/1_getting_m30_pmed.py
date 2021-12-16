import os
import logging
from glob import glob
import numpy as np
import pandas as pd
from rich.progress import track
import hydra
from omegaconf import DictConfig

log = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="dataset")
def main(cfg: DictConfig) -> None:
    log.info("=" * 80)
    log.info(" " * 20 + "Getting M30 measure points")
    log.info("=" * 80)

    if not os.path.exists(cfg.results):
        os.makedirs(cfg.results)

    log.info("Loading source files...")
    pmed_csv_list = glob(os.path.join(cfg.pmed_path, "*csv"))
    dfs = [pd.read_csv(csv, delimiter=";", encoding="latin1")
           for csv in pmed_csv_list]
    ddfs = [df[df[df.columns[0]] == 'M30'] for df in dfs]

    # Check for unique id's accross the list
    log.info("Ckecking for unique id's...")
    pts1 = ddfs[0]['id'].unique()
    for df in ddfs:
        pts2 = df['id'].unique()
        pts1 = list(set(pts1).intersection(pts2))

    pts_pmed = pts1
    num_pmed = len(pts_pmed)
    log.info(f"Number of pmed to be considered: {num_pmed}")
    # Save selected pmed's
    with open(os.path.join(cfg.results, cfg.pmed_list_name), 'w') as f:
        [f.write(str(pts)+',') for pts in pts_pmed]

    # Columns of interest
    log.info("Filter dataframe by selected pmed's...")
    cols_of_interest = ["id", "utm_x", "utm_y", "longitud", "latitud"]
    res = pd.concat([df.apply(lambda x: x if x[2] in pts_pmed else None, axis=1)
                    for df in ddfs], ignore_index=True)[cols_of_interest].dropna().drop_duplicates()
    res.to_csv(os.path.join(
        cfg.results, cfg.pmed_locations_name), index=False)

    # Calculate square matrix of distances
    log.info("Calculate square matrix distances...")
    to = []
    fromm = []
    cost = []

    for i, idx in enumerate(track(res.index, description="Calculating distances")):
        for j, jdx in enumerate(res.index):
            to.append(res.loc[idx, "id"])
            fromm.append(res.loc[jdx, "id"])
            cost.append(np.sqrt((res.loc[idx, "utm_x"] - res.loc[jdx, "utm_x"])**2 +
                                (res.loc[idx, "utm_y"] - res.loc[jdx, "utm_y"])**2))

    pd.DataFrame(data={"to": to, "from": fromm, "cost": cost}).to_csv(
        os.path.join(cfg.results, cfg.pmed_distances_name), index=False)


if __name__ == "__main__":
    main()

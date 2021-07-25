""" generate_dataset.py

This module contain routines for creating and interacting with a mongodb database.
"""
import os
import logging
import hydra
from omegaconf import DictConfig
import pandas as pd

log = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:

    log.info('=' * 80)
    log.info(' ' * 20 + 'Generate dataset')
    log.info('=' * 80)

    start_date = pd.to_datetime(f"{cfg.start_date}", format='%d-%m-%Y')
    end_date = pd.to_datetime(f"{cfg.end_date}", format='%d-%m-%Y')
    
    log.info('Generating dataframes from ' + start_date.strftime('%d-%m-%Y %H:%M:S'))
    log.info('Generating dataframes to ' + end_date.strftime('%d-%m-%Y %H:%M:S'))



if __name__ == "__main__":
    main()
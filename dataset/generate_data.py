""" generate_dataset.py

This module contain routines for creating and interacting with a mongodb database.
"""
import os
import logging
import hydra
from omegaconf import DictConfig
import pandas as pd
from pymongo import MongoClient

log = logging.getLogger(__name__)



def connect_mongo_daemon(host=None, port=None):
    if host is None and port is None:
        client = MongoClient()
    else:
        client = MongoClient(host, port)
    return client


def get_mongo_database(client, dbname):
    return client[dbname]


def get_mongo_collection(db, collection):
    return db[collection]


@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:

    log.info('=' * 80)
    log.info(' ' * 20 + 'Generate dataset')
    log.info('=' * 80)

    start_date = pd.to_datetime(f"{cfg.start_date}", format='%d-%m-%Y')
    end_date = pd.to_datetime(f"{cfg.end_date}", format='%d-%m-%Y')
    
    log.info('Generating dataframes from ' + start_date.strftime('%d-%m-%Y %H:%M:S'))
    log.info('Generating dataframes to ' + end_date.strftime('%d-%m-%Y %H:%M:S'))

    # Connect to the database
    logging.info('Connecting to the database...')
    client = connect_mongo_daemon(host=cfg.host, port=cfg.port)
    
    traffic_db = client['traffic']
    pmed_coll = traffic_db['pmed']
    start = start_date
    total_pmeds = []
    for it, month in enumerate(pd.date_range(start_date, end_date, freq='M')):
        log.info(f"Looking for pmeds between " + start.strftime('%d-%m-%Y %H:%M:S') + " and " + month.strftime('%d-%m-%Y %H:%M:S'))
        if it == 0:
            cursor = pmed_coll.find({"$and": [{"date": {"$gte": start, "$lte": month}}, {"tipo_elem": "M30"}]})
            total_pmeds = [res['id'] for res in cursor]
        else:
            cursor = pmed_coll.find({"$and": [{"date": {"$gte": start, "$lte": month}}, {"tipo_elem": "M30"}]})
            monthly_pmeds = [res['id'] for res in cursor]
            total_pmeds = list(set(total_pmeds).intersection(monthly_pmeds))
        start = month

    log.info('Getting UTM coordinates for selected pmeds...')
    rows = []
    for id in total_pmeds:
        result = list(pmed_coll.find({"id": id}))
        rows.append({"id": id, "utm_x": result[0]["utm_x"], "utm_y": result[0]["utm_y"]})

    log.info("Saving selected pmed info...")
    pd.DataFrame(rows).to_csv(os.path.join(cfg.results, "M30_stations.csv"))

    log.info("Script finished")

if __name__ == "__main__":
    main()
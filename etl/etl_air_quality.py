import os
import logging
import hydra
from omegaconf import DictConf
from glob import glob
import pandas as pd

from tools import database as db
from tools.etl_utils import insert_pollution_docs_to_db

log = logging.getLogger(__name__)


def get_air_stations(source_path):
    stations = pd.read_csv(os.path.join(source_path, 'estaciones', 'pollution_stations.csv'),
                           encoding='latin1', delimiter=';')
    stations.drop(columns=stations.columns[0], inplace=True)
    return [val for val in stations.to_dict(orient='index').values()]


@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConf) -> None:

    log.info('='*80)
    log.info(' '*20 + 'ETL air quality')
    log.info('=' * 80)

    if cfg.air.source_path is None:
        raise ValueError("Source path for input data must be provided.")
    log.info('Data sourced from :' + cfg.air.source_path)

    # Put into the database
    log.info('Connecting with the database')
    client = db.connect_mongo_daemon(host=cfg.host, port=cfg.port)
    pollutiondb = db.get_mongo_database(client, 'pollution')

    # Load station info
    log.info('Inserting pollution stations data...')
    stations = get_air_stations(cfg.air.source_path)
    result = db.insert_many_documents(pollutiondb, 'stations', stations)

    # First, all the data are in txt. It must be converted to csv
    # for easier manipulation
    insert_pollution_docs_to_db(os.path.join(cfg.air.source_path, 'contaminantes',  'historico'),
                                pollutiondb, 'pollutants')

    # Create index
    pollutants_coll = db.get_mongo_collection(pollutiondb, 'pollutants')
    pollutants_coll.create_index([("dates", -1), ("magnitud", 1)], name='pollutant_index')
    pollutants_coll.create_index([("station", 1), ("magnitud", 1)], name='pollutant_plus_station_index')
    pollutants_coll.create_index([("station", 1), ("dates", -1), ("magnitud", 1)], name='pollutant_all_index')

    # Closing script
    log.info('ETL air_quality process finished.')
    log.info('=' * 80)


if __name__ == '__main__':
    main()

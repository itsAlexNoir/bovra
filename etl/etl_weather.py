#!/usr/bin/env python
""" etl_weather.py

This scipt loads weather data from sources and load it to a mongo database.
Usage:
    etl_weather.py (-h | --help)
    etl_weather.py --source_path=<path> --port=<port> [--host=<host> --include_stations]

Options:
    -h --help               Show this screen
    --source_path=<path>    Path to the data source
    --host=<host>           Host address of the MongoDB server [default: localhost]
    --port=<Port>           Port of the MongoDB server
    --include_stations      Whether to insert or not station information.
"""

import os
import logging
import datetime
from glob import glob
import hydra
from omegaconf import DictConfig
import pandas as pd

from tools import database as db

log = logging.getLogger(__name__)

def get_weather_stations(source_path):
    weather_stations_file = os.path.join(source_path, 'estaciones_meteo.json')
    stations = pd.read_json(weather_stations_file)
    #stations['lon'] = stations['longitud'].apply(lambda x: -1 * float(x[:-1]) if x[-1] == 'W' else float(x[:-1]))
    #stations['lon'] = stations['longitud'].apply(lambda x: -1 * float(x[:-1]) if x[-1] == 'W' else float(x[:-1]))
    #with open(weather_stations_file, 'r') as f:
    #    return json.load(f)
    return [sta for sta in stations.to_dict(orient='index').values()]


def get_weather_historic(source_path, wilcard):
    return glob(os.path.join(source_path, wilcard+'*'))

@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:

    log.info('='*80)
    log.info(' '*20 + 'ETL weather')
    log.info('=' * 80 + '\n')

    # Connect with the mongo daemon
    log.info('Connecting to the database client')
    client = db.connect_mongo_daemon(host=cfg.host, port=cfg.port)
    # First, create the database. It's dubbed "aire"
    log.info('Creating weather collection in database')
    weather = db.get_mongo_database(client, 'weather')

    ## CLIMATE INFO
    # Let's import climate data from disk and insert them into the database
    log.info('Creating climate collection')
    stations_coll = db.get_mongo_collection(weather, 'stations')

    if cfg.weather.include_stations:
        log.info('ETL weather stations...')
        stations = get_weather_stations(cfg.weather.source_path)
        result = db.insert_many_documents(weather, 'stations', stations)

    log.info('Inserting weather data...')
    log.info('Starting at ' + datetime.datetime.now().strftime('%d/%m/%Y - %H:%m:%s'))
    weather_files = get_weather_historic(cfg.weather.source_path, 'datos_clima*')
    for file in weather_files:
        weather_data = pd.read_json(file)
        weather_data['date'] = pd.to_datetime(weather_data['fecha'], format='%Y-%m-%d')
        weather_dict = [val for val in weather_data.to_dict(orient='index').values()]
        log.info('Inserting from {} into weather historic collection...'.format(file))
        result = db.insert_many_documents(weather, 'historic', weather_dict)
    log.info('Creation database finished at ' + datetime.datetime.now().strftime('%d/%m/%Y - %H:%m:%s'))

    log.info('=' * 80)
    log.info('Weather info insertion finished')
    log.info('='*80)


if __name__ == '__main__':
    main()

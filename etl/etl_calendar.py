""" 
    etl_calendar.py
Usage:
    etl_calendar.py (-h | --help)
    etl_calendar.py --source_path=<path> --port=<port> [--host=<host>]

Options:
    -h --help               Show this screen.
    --source_path=<path>    Path to the source data
    --host=<host>           Host address of the MongoDB server [default: localhost]
    --port=<Port>           Port of the MongoDB server
"""
import os
import logging
import hydra
from omegaconf import DictConfig
import pandas as pd

from tools import database as db

log = logging.getLogger(__name__)

def get_calendar_from_source(source_path):
    df = pd.read_csv(os.path.join(source_path, 'calendario.csv'),
                     encoding='latin1', delimiter=';')
    df['date'] = df['Dia'].apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y'))
    df['Día'] = df['date'].apply(lambda x: x.day)
    df['Mes'] = df['date'].apply(lambda x: x.month)
    df['Año'] = df['date'].apply(lambda x: x.year)

    return [val for val in df.drop(columns=['Dia']).to_dict(orient='index').values()]


def insert_calendar_documents(db, coll, entries):
    for entry in entries:
        if db[coll].find({"date": {"$eq":entry['date']}}).count()==0:
            db.insert_one_document(db, coll, entry)


@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:
    log.info('='*80)
    log.info(' ' * 20 + 'ETL calendar')
    log.info('=' * 80)

    log.info('Data sourced from :' + cfg.calendar.source_path)

    log.info('Extracting data...')
    calendar = get_calendar_from_source(cfg.calendar.source_path)

    # Put into the database
    client = db.connect_mongo_daemon(host=cfg.host, port=cfg.port)
    calendardb = db.get_mongo_database(client, 'calendar')
    log.info('Inserting calendar data...')
     
    result = insert_calendar_documents(calendardb, 'calendar', calendar)

    log.info('ETL calendar process finished.')
    log.info('=' * 80)


if __name__ == '__main__':
    main()

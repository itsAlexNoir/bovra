#!/usr/bin/env python
""" generate_dataset.py

This module contain routines for creating and interacting with a mongodb database.
"""

import os
import logging
import datetime
import pandas as pd
import time
import pickle
from tqdm import tqdm
from functools import partial
import multiprocessing as mp
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

from tools import database as db
from tools.etl_utils import sustancias

# lon/lat coordinates for Puerta del Sol (Madrid city central point)
PUERTA_SOL = [-3.703339, 40.416729]

def define_flags():
    flags.DEFINE_list(name='start_date', default=None, help='Initial date of the request')
    flags.DEFINE_list(name='end_date', default=None, help='Initial date of the request')
    flags.DEFINE_string(name='output_folder', default=None, help='Path to the output folder')
    flags.DEFINE_integer(name='station', default=None, help='Pollution station number to be selected')
    flags.DEFINE_string(name='host', default='localhost', help='Host for Mongo DB database.')
    flags.DEFINE_integer(name='port', default=27019, help='Port connection to Mongo DB database.')


def get_calendar_features(database, start, end):
    calendar_coll = database['calendar']['calendar']
    calendar = [cal for cal in calendar_coll.find({"date": {"$gte": start, "$lte": end}})]
    one_hot_festivo = pd.Series(
        [0 if cal['laborable / festivo / domingo festivo'] == 'laborable' else 1 for cal in calendar])
    calendar_date = pd.Series(['{:02d}-{:02d}-{:4d}'.format(cal["Día"], cal["Mes"], cal["Año"]) for cal in calendar])
    calendar_date = pd.to_datetime(calendar_date, format='%d-%m-%Y')
    day = calendar_date.apply(lambda x: x.day)
    month = calendar_date.apply(lambda x: x.month)
    year = calendar_date.apply(lambda x: x.year)
    cal = pd.DataFrame(data={"date": calendar_date, "day": day, "month": month, "year": year,
                             "festivo": one_hot_festivo})
    dd = pd.date_range(min(calendar_date), max(calendar_date), freq='H')
    vals_per_hour = [cal[(cal['day'] == d.day) & (cal['month'] == d.month) & (cal['year'] == d.year)]['festivo'].values[0] for d in dd]
    return pd.DataFrame({'date': dd, 'festivo': vals_per_hour})


def get_weather_features(database, start, end):
    weather_coll = database['weather']['historic']
    weather_cols = ['tmed', 'prec', 'velmedia', 'date']
    weather_daily = pd.DataFrame([w for w in weather_coll.find({"date": {"$gte": start, "$lte": end}})])
    weather_daily = weather_daily[weather_cols]
    weather_daily['day'] = weather_daily['date'].apply(lambda x: x.day)
    weather_daily['month'] = weather_daily['date'].apply(lambda x: x.month)
    weather_daily['year'] = weather_daily['date'].apply(lambda x: x.year)
    weather_daily['prec'] = weather_daily['prec'].apply(lambda x: x.replace(',', '.'))
    weather_daily['tmed'] = weather_daily['tmed'].fillna('20,0').apply(lambda x: x.replace(',', '.'))
    weather_daily['velmedia'] = weather_daily['velmedia'].fillna('0,0').apply(lambda x: x.replace(',', '.'))
    weather_daily['prec'] = weather_daily['prec'].apply(lambda x: 0.0 if x == 'Ip' else x)


    dd = pd.date_range(min(weather_daily['date']), max(weather_daily['date']), freq='H')
    vals_per_hour = [weather_daily[(weather_daily['day'] == d.day) & (weather_daily['month'] == d.month) &
                                   (weather_daily['year'] == d.year)][weather_cols].values[0] for d in dd]
    tmed = [v[0] for v in vals_per_hour]
    prec = [v[1] for v in vals_per_hour]
    velmed = [v[2] for v in vals_per_hour]
    return pd.DataFrame({'date': dd, 'tmed': tmed, 'prec': prec, 'velmed': velmed})


def get_nearby_traffic_pmed(database, distances):
    # Load pollution stations
    stations_coll = database['pollution']['stations']
    stations = [st for st in stations_coll.find()]

    # Load traffic measure points
    pmed_coll = database['traffic']['pmed']
    closest = {}
    for station in tqdm(stations):
        logging.info('Pulling nearest stations for ' + station['ESTACION'])
        closest[station['CODIGO_CORTO']] = {}
        for key, dist in distances.items():
            pmed_list = [query for query in pmed_coll.find({"location":
                                                                {"$near":
                                                                     {"$geometry": {"type": "Point",
                                                                                    "coordinates": [station['LONGITUD'],
                                                                                                    station[
                                                                                                        'LATITUD']]},
                                                                      "$minDistance": dist[0],
                                                                      "$maxDistance": dist[1]
                                                                      }
                                                                 }
                                                            })]
            if len(list(pmed_list)) != 0:
                closest[station['CODIGO_CORTO']][key] = pd.DataFrame(list(pmed_list))['id'].dropna().unique()
    return closest


def retrieve_traffic_data_by_hour(query_date, close, coll):
    density = []
    #logging.info('Getting traffic data for ' + query_date.strftime('%d-%m-%Y %H:%M:%s'))
    for station in close.keys():
        #logging.info('For pollution station code {}'.format(station))
        dense_row = {}
        for dist in close[station].keys():

            try:
                density_hour = pd.DataFrame([den for den in coll.find({"date": query_date, "$or": [{"idelem": {"$in": list(map(int, close[station][dist]))}}, {"id": {"$in": list(map(int, close[station][dist]))}}]})])
                dense_row.update({'date': query_date, 'reference_station': station,
                            'intensidad_media_' + dist: density_hour['intensidad'].mean(),
                            'intensidad_min_' + dist: density_hour['intensidad'].min(),
                            'intensidad_max_' + dist: density_hour['intensidad'].max(),
                            'carga_media_' + dist: density_hour['carga'].mean(),
                            'carga_min_' + dist: density_hour['carga'].min(),
                            'carga_max_' + dist: density_hour['carga'].max(),
                            'ocupacion_media_' + dist: density_hour['ocupacion'].mean(),
                            'ocupacion_min_' + dist: density_hour['ocupacion'].min(),
                            'ocupacion_max_' + dist: density_hour['ocupacion'].max(),
                            'vmed_media_' + dist: density_hour['vmed'].mean(),
                            'vmed_min_' + dist: density_hour['vmed'].min(),
                            'vmed_max_' + dist: density_hour['vmed'].max()})
            except KeyError:
                logging.error('Data not found for {} at '.format(dist) + query_date.strftime("%Y-%m-%d %H:%M:%S"))
                pass
        density.append(dense_row)
    return density


def get_density_traffic_data(database, start, end, close):
    density_coll = database['traffic']['density']
    time_axis = pd.date_range(start=start, end=end, freq='H')
    mapfunc = partial(retrieve_traffic_data_by_hour, close=close, coll=density_coll)
    logging.info('Starting retrieving traffic data at ' + datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
    with ThreadPool() as pool:
        density = pool.map_async(mapfunc, time_axis)
        while not density.ready():
            time.sleep(0.01)
    logging.info('Ending retrieving traffic data at ' + datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
    return pd.DataFrame([item for sublist in density.get() for item in sublist])


def get_air_quality_data(database, start, end):
    air_coll = database['pollution']['pollutants']
    time_axis = pd.date_range(start=start, end=end, freq='H')
    interesting_cols = ['station', 'magnitud', 'date', 'value']
    air_data = []
    for query_date in tqdm(time_axis):
        if FLAGS.station is not None:
            air = pd.DataFrame(list(air_coll.find({"dates": query_date, "station": FLAGS.station}))).rename(
                columns={"dates": "date"})
        else:
            air = pd.DataFrame(list(air_coll.find({"dates": query_date}))).rename(columns={"dates": "date"})
        #try:
        air_data.append(air[interesting_cols])
        #except KeyError:
        #    print(query_date)
        #    pass
    return pd.concat([air for air in air_data], ignore_index=True)


def sort_pollutants_in_columns(data):
    pollutants_per_station = data['magnitud'].unique()
    air = data[data['magnitud'] == pollutants_per_station[0]].\
        rename(columns={'value': sustancias[pollutants_per_station[0]]}).drop(columns='magnitud').set_index('date')
    for pol in pollutants_per_station[1:]:
        air = air.join(data[data['magnitud'] == pol]
                       .rename(columns={'value': sustancias[pol]}).drop(columns='magnitud').set_index('date'))

    return air.reset_index()


def main(argv):
    logging.info('-' * 80)
    logging.info(' ' * 20 + 'Generate dataset!')
    logging.info('_' * 80 + '\n')

    start_date = pd.to_datetime('{}-{}-{}'.format(FLAGS.start_date[1], FLAGS.start_date[1], FLAGS.start_date[2]),
                                format='%d-%m-%Y')
    end_date = pd.to_datetime('{}-{}-{}'.format(FLAGS.end_date[0], FLAGS.end_date[1], FLAGS.end_date[2]),
                              format='%d-%m-%Y')
    logging.info('Generating dataframes from ' + start_date.strftime('%d-%m-%Y %H:%M:S'))
    logging.info('Generating dataframes to ' + end_date.strftime('%d-%m-%Y %H:%M:S'))

    # Connect to the database
    logging.info('Connecting to the database...')
    client = db.connect_mongo_daemon(host=FLAGS.host, port=FLAGS.port)

    # Retrieving calendar info
    logging.info('Extracting holidays info from calendar...')
    calendar_df = get_calendar_features(client, start_date, end_date)

    # Weather info
    logging.info('Extracting weather data...')
    weather_df = get_weather_features(client, start_date, end_date)

    # Get air quality data
    logging.info('Retrieve info for air quality')
    air_data = get_air_quality_data(client, start_date, end_date)
    if FLAGS.station is not None:
        air_data.drop(columns=['station'], inplace=True)
    air_data = sort_pollutants_in_columns(air_data)

    #air_data.to_pickle(os.path.join(FLAGS.output_folder, 'air_df.pkl'))

    # Get all traffic pmed in a certain radius from pollution station
    distances = {'0_500m': [0, 500], '500m_1km': [500, 1000]}
    closest = get_nearby_traffic_pmed(client, distances)

    # Extract traffic density data
    logging.info('Retrieve info for a density info')
    if FLAGS.station is not None:
        closest = {FLAGS.station: closest[FLAGS.station]}
    traffic_data = get_density_traffic_data(client, start_date, end_date, closest)
    if FLAGS.station is not None:
        traffic_data.drop(columns=['reference_station'], inplace=True)
    # Join all the pieces all together
    data_df = calendar_df.set_index('date').join(air_data.set_index('date'))
    data_df = data_df.join(weather_df.set_index('date'))
    data_df = data_df.join(traffic_data.set_index('date')).reset_index()

    if FLAGS.station is not None:
        data_df.to_pickle(os.path.join(FLAGS.output_folder,
                                       'data_station_{}_from_{}_to_{}_df.pkl'.format(FLAGS.station,
                                                                                     start_date.strftime('%d-%m-%Y'),
                                                                                     end_date.strftime('%d-%m-%Y'))))
    else:
        data_df.to_pickle(os.path.join(FLAGS.output_folder, 'data_all_stations_df.pkl'))

    logging.info('-' * 80)
    logging.info(' ' * 20 + 'Finished dataset generation!')
    logging.info('_' * 80 + '\n')


if __name__ == '__main__':
    app.run(main)

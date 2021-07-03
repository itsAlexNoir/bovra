""" etl_utils.py

This module contain routines for cleaning txt files from the air pollution dataset
"""
__author__ = "Alejandro de la Calle"
__copyright__ = "Copyright 2019"
__credits__ = [""]
__license__ = ""
__version__ = "0.1"
__maintainer__ = "Alejandro de la Calle"
__email__ = "alejandrodelacallenegro@gmail.com"
__status__ = "Development"


import os
import re
import logging
from glob import glob
import numpy as np
import pandas as pd

from tools import database as db

log = logging.getLogger(__name__)

##############################################
## Dictionaries useful for pollution and weather data

estaciones_aire = {'28079001': 'Plz. Recoletos', '28079002': 'Glta. Carlos V', '28079003': 'Pza. Carmen (<2011)',
              '28079035': 'Plz. Carmen', '28079004': 'Plz. España', '28079005': 'Barrio del Pilar (<2011)',
              '28079039': 'Barrio del Pilar', '28079006': 'Plz. Dr. Marañon', '28079007': 'Plz. M. de Salamanca',
              '28079008': 'Escuelas Aguirre', '28079009': 'Plz. Luca de Tena', '28079010': 'Cuatro Caminos (<2011)',
              '28079038': 'Cuatro Caminos', '28079011': 'Av. Ramón y Cajal', '28079012': 'Plz. Manuel Becerra',
              '28079013': 'Vallecas (<2011)', '28079040': 'Vallecas', '28079014': 'Plz. Fernández Ladreda (<2009)',
              '28079015': 'Plz. Castilla (<2008)', '28079016': 'Arturo Soria', '28079017': 'Villaverde Alto',
              '28079018': 'C/ Farolillo', '28079019': 'Huerta Castañeda', '28079020': 'Moratalaz (<2011)',
              '28079036': 'Moratalaz', '28079021': 'Pza. Cristo Rey', '28079022': 'P. Pontones', '28079023': 'Final Alcalá',
              '28079024': 'Casa campo', '28079025': 'St. Eugenia', '28079026': 'Urb. Embajada-Barajas (<2010)',
              '28079027': 'Barajas', '28079047': 'Mendez Álvaro', '28079048': 'P. Castellana', '28079049': 'Retiro',
              '28079050': 'Plz. Castilla', '28079054': 'Ensanche Vallecas', '28079055': 'Urb. Embajada-Barajas',
              '28079056': 'Plz. Fdz Ladreda', '28079057': 'Sanchinarro', '28079058': 'El Pardo',
              '28079059': 'Parque Juan Carlos I', '28079086': 'Tres Olivos (<2011)', '28079060': 'Tres Olivos',
              '28079099': 'Media global'}

sustancias = {1: 'SO2', 6: 'CO', 7: 'NO', 8: 'NO2', 9: 'PM2.5', 10: 'PM10', 12: 'NOx', 14: 'O3',
              20: 'TOL', 30: 'BEN', 35: 'EBE', 37: 'MXY', 38: 'PXY', 39: 'OXY', 42: 'TCH', 43: 'CH4',
              44: 'NMHC', 58: 'HCl'}

# sustancias = {'01': 'SO2', '06': 'CO', '07': 'NO', '08': 'NO2', '09': 'PM2.5', '10': 'PM10', '12': 'NOx', '14': 'O3',
#               '20': 'TOL', '30': 'BEN', '35': 'EBE', '37': 'MXY', '38': 'PXY', '39': 'OXY', '42': 'TCH', '43': 'CH4',
#               '44': 'NMHC', '58': 'HCl'}

mes = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
       9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}

estaciones_meteo = {'Retiro': '3195', 'Aeropuerto': '3129', 'Ciudad_Universitaria': '3194U', 'Cuatro_Vientos': '3196'}

################################################################################


def dms2dec(dms_str):
    """  Return decimal representation of DMS

    Minutes, Seconds formatted coordinate strings to decimal.

    Formula:
    DEC = (DEG + (MIN * 1/60) + (SEC * 1/60 * 1/60))

    Assumes S/W are negative.

    dms2dec(utf8(48°53'10.18"N))
    48.8866111111F

    dms2dec(utf8(2°20'35.09"E))
    2.34330555556F

    dms2dec(utf8(48°53'10.18"S))
    -48.8866111111F

    dms2dec(utf8(2°20'35.09"W))
    -2.34330555556F
    """
    dms_str = re.sub(r'\s', '', dms_str)
    sign = -1 if re.search('[swSW]', dms_str) else 1
    numbers = [*filter(len, re.split('\D+', dms_str, maxsplit=4))]

    degree = numbers[0]
    minute = numbers[1] if len(numbers) >= 2 else '0'
    second = numbers[2] if len(numbers) >= 3 else '0'
    frac_seconds = numbers[3] if len(numbers) >= 4 else '0'

    second += "." + frac_seconds
    return sign * (int(degree) + float(minute) / 60 + float(second) / 3600)


def parse_pollution_txt(txt_file):
    horasstr = ['H{:02d}'.format(h) for h in range(1, 25)]
    valstr = ['V{:02d}'.format(v) for v in range(1, 25)]
    horasstr[-1] = 'H00'
    valstr[-1] = 'V00'

    cod_provincia = []
    cod_municipio = []
    cod_estacion = []
    cod_magnitud = []
    cod_tecnica = []
    anno = []
    mes = []
    dia = []
    horas = []
    val = []

    with open(txt_file) as f:
        lines = f.readlines()
        for line in lines:
            line.replace('\n', '')
            cod_provincia.append(line[:2])
            cod_municipio.append(line[2:5])
            cod_estacion.append(line[5:8])
            cod_magnitud.append(line[8:10])
            cod_tecnica.append(line[10:12])
            anno.append(line[14:16])
            mes.append(line[16:18])
            dia.append(line[18:20])
            hh = [line[20 + h * 6:25 + h * 6] for h in range(24)]
            horas.append(hh)
            vv = [line[25 + v * 6] for v in range(24)]
            val.append(vv)

    # Transform hhh to have only 24 columns (one per hour), and the length of each column
    # is the number of measures
    horas = list(zip(*horas))
    val = list(zip(*val))

    measures = dict(zip(horasstr, horas))
    validez = dict(zip(valstr, val))

    cols1 = {'provincia': cod_provincia, 'municipio': cod_municipio, 'station': cod_estacion,
             'magnitud': cod_magnitud, 'tecnica': cod_tecnica,
             'year': anno, 'month': mes, 'day': dia}

    cols = {**cols1, **measures, **validez}
    df = pd.DataFrame(data=cols)
    df['year'] = df['year'].apply(lambda x: '20' + x)
    df['magnitud'] = df['magnitud'].astype(np.int32)
    df['station'] = df['station'].astype(np.int32)
    pollution_month = []
    for hora in horasstr:
        common_cols = ['station', 'magnitud']
        date_cols = ['year', 'month', 'day', 'hour']
        df['hour'] = hora[-2:]
        dd = df[common_cols]
        dd['dates'] = pd.to_datetime(df.loc[:, date_cols])
        if hora == 'H00':
            dd['dates'] += pd.Timedelta(1, unit='day')
        dd['value'] = df[hora].astype(np.float32)
        pollution_month.append(list(dd.to_dict(orient='index').values()))

    return [item for sublist in pollution_month for item in sublist]

def parse_pollution_csv(csv_file):
    horasstr = ['H{:02d}'.format(h) for h in range(1, 25)]
    # valstr = ['V{:02d}'.format(v) for v in range(1, 25)]
    horasstr[-1] = 'H00'
    #valstr[-1] = 'V00'

    df = pd.read_csv(csv_file, delimiter=';')
    df.rename(columns={'H24': 'H00', 'V24': 'V00',
                       'ANO': 'year', 'MES': 'month',
                       'DIA': 'day', 'ESTACION': 'station',
                       'MAGNITUD': 'magnitud'}, inplace=True)
    pollution_month = []
    for hora in horasstr:
        common_cols = ['station', 'magnitud']
        date_cols = ['year', 'month', 'day', 'hour']
        df['hour'] = hora[-2:]
        dd = df[common_cols]
        dd['dates'] = pd.to_datetime(df.loc[:, date_cols])
        if hora == 'H00':
            dd['dates'] += pd.Timedelta(1, unit='day')
        dd['value'] = df[hora]
        pollution_month.append(list(dd.to_dict(orient='index').values()))

    return [item for sublist in pollution_month for item in sublist]


def insert_pollution_docs_to_db(source_path, database, coll):
    folders = sorted(glob.glob(os.path.join(source_path, 'Anio*')))
    [log.info('Getting files from year {}'.format(os.path.basename(folder)[4:8])) for folder in folders]

    txt_files = [sorted(glob.glob(os.path.join(folder, '*txt'))) for folder in folders]
    # Flatten given list
    txt_files = [item for sublist in txt_files for item in sublist]

    csv_files = [sorted(glob.glob(os.path.join(folder, '*csv'))) for folder in folders]
    # Flatten given list
    csv_files = [item for sublist in csv_files for item in sublist]

    log.info('Parsing data...')
    for file in txt_files:
         log.info('Parsing ' + os.path.basename(file))
         pollutants = parse_pollution_txt(file)
         result = db.insert_many_documents(database, coll, pollutants)

    for file in csv_files:
        log.info('Parsing ' + os.path.basename(file))
        pollutants = parse_pollution_csv(file)
        result = db.insert_many_documents(database, coll, pollutants)

    log.info('Insertion finished')
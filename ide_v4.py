'''
V4 
26 - Julio -2024
Corregido el bug: No es posible hacer login
Simplificado el código para no usar librerias

'''

import json
import configparser
import paho.mqtt.publish as publish

from datetime import date, timedelta
from datetime import datetime

import logging
from logging.handlers import RotatingFileHandler

import requests
from requests import Session

__domain = "https://www.i-de.es"
__login_url = __domain + "/consumidores/rest/loginNew/login"
__obtener_periodo_url = (__domain + "/consumidores/rest/consumoNew/obtenerDatosConsumoPeriodo/fechaInicio/{}00:00:00/fechaFinal/{}00:00:00/") 

parser = configparser.ConfigParser()
parser.read('config_iberdrola.ini')

__headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
    "Origin": "https://www.i-de.es/",
    "accept": "application/json; charset=utf-8",
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
}


''' Niveles de logging
Para obtener _TODO_ el detalle: level=logging.DEBUG
Para comprobar los posibles problemas level=logging.WARNINg
Para comprobar el funcionamiento: level=logging.INFO
'''
logging.basicConfig(
        level=logging.DEBUG,
        handlers=[RotatingFileHandler('./logs/log_datadis.log', maxBytes=10000000, backupCount=4)],
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')

def consumption_hour(dataj):
    hour_kwh = []
    for x in dataj["y"]["data"][0]:
        if x:
            hour_kwh.append(float(x["valor"]))
    return hour_kwh

def mqtt_tx(client,s_value):
    # logging.debug(client + "  " + s_register + "  " + s_value)
    # Parseo de las variables
    mqtt_topic_prefix = parser.get('mqtt_broker','mqtt_topic_prefix')
    mqtt_ip = parser.get('mqtt_broker','mqtt_ip')
    mqtt_login = parser.get('mqtt_broker','mqtt_login')
    mqtt_password = parser.get('mqtt_broker','mqtt_password')

    mqtt_auth = { 'username': mqtt_login, 'password': mqtt_password }
    response = publish.single(mqtt_topic_prefix + "/" + client, s_value, hostname=mqtt_ip, auth=mqtt_auth)

def save_reading_register(rrs0):
    rr_path = "registers/reading_register.txt"
    writing =open(rr_path, "w", encoding="utf-8")
    json.dump(rrs0,writing)
    writing.close()

def open_reading_register():
    rr_path = "registers/reading_register.txt"
    lectura=open(rr_path, "r", encoding="utf-8")
    data = json.load(lectura)
    lectura.close()
    return data


rrs = open_reading_register() # Reading 

for rr in rrs:

    session = Session()
    
    login_data = ('["{}","{}",null,"Linux -","PC","Chrome 77.0.3865.90","0","","s"]'.format(rr["login"], rr["password"]))

    # {"login": "aa@bb.com", "password": "pas","energy": 1.1, "last": "2024-04-03T00:00:00" }
    try: 
        response = session.request("POST", __login_url, data=login_data, headers=__headers)
        print('resultado: ')
        print(response.status_code)

    except Exception as ex:
        print("Ha habido una excepcion", type(ex))
    
    from_date = datetime.fromisoformat(rr["last"])
    # from_date = datetime.fromisoformat("2023-01-01T00:00:00")
    logging.debug(from_date)
    # <class 'datetime.datetime'>
    
    until_date = date.today()
    # until_date = datetime.fromisoformat("2004-01-01T00:00:00")
    # until_date = date.today() - timedelta(days=1)

    # La consulta se hace de días enteros
    # consumo = await connection.consumption(from_date, until_date)

    consumo_raw = {}
    start_str = from_date.strftime("%d-%m-%Y")
    end_str = until_date.strftime("%d-%m-%Y")

    logging.debug("consumo_raw_j:")
    
    try:
        consumo_raw = session.request("GET", __obtener_periodo_url.format(start_str,end_str), headers=__headers,)
        consumo_raw_j = consumo_raw.json()
        logging.debug(consumo_raw_j)

    except Exception as ex:
        print("Ha habido una excepción", type(ex))

    if consumo_raw_j != {}:
        consumption_h = consumption_hour(consumo_raw_j)
        logging.debug("consumption_h:")
        logging.debug(consumption_h)

        date_s = consumo_raw_j["fechaPeriodo"]

        init_d = datetime(int(date_s[6:10]),int(date_s[3:5]),int(date_s[0:2]),
                        int(date_s[10:12]),int(date_s[13:15]),int(date_s[16:18]))
        logging.debug("init_d: ")
        logging.debug(init_d)
        # <class 'datetime.datetime'>
        e_a = float(rr["energy"]) # e_a  energy acumulated
        ener_time = from_date
        i=0
        for n in consumption_h:
            i += 1
            ener_time = init_d + timedelta(seconds=(3600 * i))
            if(ener_time > from_date):
                e_a = e_a + float(n)
                # e_h energy in an hour
                data_tx = {"name": rr["name"],
                           "time":str(ener_time.replace(microsecond=0).isoformat()),
                           "e_h":n,
                           "energy":e_a}
                logging.info(rr["name"] + " - " + str(data_tx))
                mqtt_tx(rr["name"],str(data_tx))
            else:
                logging.debug("NO-TX " + str(ener_time.replace(microsecond=0).isoformat()) +" "+ str(n))
                           
        rr["last"] = str(ener_time.replace(microsecond=0).isoformat())
        rr["energy"] = e_a

    session.close() 
    
save_reading_register(rrs)
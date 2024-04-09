'''
TAREAS
Incluir sistema de logs
    ok Cambiar todos los print()
    Revisar la prioridad de los mensajes

Refactorizar
    Eliminar ficheros no usados


'''

'''
NOTAS
consumoNew/obtenerDatosConsumoPeriodo/fechaInicio/23-03-202400:00:00/fechaFinal/31-03-202400:00:00/

'''


import asyncio
import fun
from fun.asyncio import AsyncIber
from fun.asyncio import open_reading_register
from fun.asyncio import save_reading_register

from datetime import date, timedelta
from datetime import datetime

import logging
from logging.handlers import RotatingFileHandler

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


async def main():
        

    rrs = open_reading_register() # Reading 

    for rr in rrs:

        connection = AsyncIber()
        

        # {"login": "aa@bb.com", "password": "pas","energy": 1.1, "last": "2024-04-03T00:00:00" }
        try: 
            await connection.login(rr["login"],rr["password"])

        except Exception as ex:
            print("Ha habido una excepción", type(ex))
        
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

        try:
            consumo_raw = await connection.consumption_raw(from_date, until_date)

        except Exception as ex:
            print("Ha habido una excepción", type(ex))

        logging.debug("consumo_raw:")
        logging.debug(consumo_raw)



        if consumo_raw != {}:
            consumption_h = await connection.consumption_hour(consumo_raw)
            logging.debug("consumption_h:")
            logging.debug(consumption_h)

            #'012345678901234567'
            #'27-03-202400:00:00'
            date_s = consumo_raw["fechaPeriodo"]

            init_d = datetime(int(date_s[6:10]),int(date_s[3:5]),int(date_s[0:2]),
                            int(date_s[10:12]),int(date_s[13:15]),int(date_s[16:18]))
            logging.debug("init_d: ")
            logging.debug(init_d)
            # <class 'datetime.datetime'>

        
            e_a = float(rr["energy"]) # e_a  energy acumulated
            ener_time = from_date
            i=0
            for n in consumption_h:
                '''{"name": "na","login": "lo@lo.com","password": "pa", "energy": 1.1,"last": "2024-04-04T00:00:00"}'''

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
                    await connection.mqtt_tx(rr["name"],str(data_tx))

                else:
                    logging.debug("NO-TX " + str(ener_time.replace(microsecond=0).isoformat()) +" "+ str(n))

            rr["last"] = str(ener_time.replace(microsecond=0).isoformat())
            rr["energy"] = e_a

        await connection.close()

    save_reading_register(rrs)

asyncio.run(main())






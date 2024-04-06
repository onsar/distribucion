import json

from deprecated.classic import deprecated

from ..exception import SessionException, ResponseException, NoResponseException, LoginException, \
    SelectContractException

try:
    import aiohttp
except ImportError:
    raise RuntimeError("AsyncIber requires aiohttp module")

from datetime import datetime
from typing import Union, Optional

# [os] +++++++++++++


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

import configparser
import paho.mqtt.publish as publish
parser = configparser.ConfigParser()

parser.read('config_iberdrola.ini')



# [os] +++++++++++++


LOGIN_URL = "loginNew/login"
WATTHOURMETER_URL = "escenarioNew/obtenerMedicionOnline/24"
ICP_STATUS_URL = "rearmeICP/consultarEstado"
CONTRACTS_URL = "cto/listaCtos/"
CONTRACT_DETAIL_URL = "detalleCto/detalle/"
CONTRACT_SELECTION_URL = "cto/seleccion/"
OBTENER_ESCENARIOS_URL = "escenarioNew/obtenerEscenariosRest/"
OBTENER_ESCENARIO_URL = "escenarioNew/refrescarEscenario/"
GUARDAR_ESCENARIO_URL = "escenarioNew/confirmarMedicionOnLine/{}/1/{}"
BORRAR_ESCENARIO_URL = "escenarioNew/borrarEscenario/"
OBTENER_PERIODO_URL = "consumoNew/obtenerDatosConsumoPeriodo/fechaInicio/{}00:00:00/fechaFinal/{}00:00:00/"
OBTENER_PERIODO_GENERACION_URL = "consumoNew/obtenerDatosGeneracionPeriodo/fechaInicio/{}00:00:00/fechaFinal/{}00:00:00/"


class AsyncIber:
    def __init__(self) -> None:
        """Iber class __init__ method."""
        self.__session = None

    async def close(self):
        if self.__session:
            await self.__session.close()



    # [os] +++++++++++++++++++++++++
    
    async def mqtt_tx(self,client,s_value):
        # logging.debug(client + "  " + s_register + "  " + s_value)
        # Parseo de las variables
        mqtt_topic_prefix = parser.get('mqtt_broker','mqtt_topic_prefix')
        mqtt_ip = parser.get('mqtt_broker','mqtt_ip')
        mqtt_login = parser.get('mqtt_broker','mqtt_login')
        mqtt_password = parser.get('mqtt_broker','mqtt_password')

        mqtt_auth = { 'username': mqtt_login, 'password': mqtt_password }
        response = publish.single(mqtt_topic_prefix + "/" + client, s_value, hostname=mqtt_ip, auth=mqtt_auth)

    # [os] +++++++++++++++++++++++++



    async def __request(
        self, path: str, data: Optional[Union[list, dict]] = None
    ) -> dict:
        logging.debug("path: " + path)
        if not self.__session:
            raise SessionException()
        if data is None:
            response = await self.__session.get(
                f"https://www.i-de.es/consumidores/rest/{path}",
                headers={"accept": "application/json; charset=utf-8"},
            )
        else:
            response = await self.__session.post(
                f"https://www.i-de.es/consumidores/rest/{path}",
                json=data,
                headers={"accept": "application/json; charset=utf-8"},
            )
        if response.status != 200:
            await self.__session.close()
            self.__session = None
            raise ResponseException(response.status)
        data = await response.json()
        if not data:
            raise NoResponseException
        logging.debug("++++++++++++++++++++++++++++")
        logging.debug(data)
        logging.debug("++++++++++++++++++++++++++++")
        return data
    

    async def login(self, user: str, password: str) -> bool:
        """Creates session with your credentials"""
        self.__session = aiohttp.ClientSession()
        payload = [
            user,
            password,
            None,
            "Linux -",
            "PC",
            "Chrome 77.0.3865.90",
            "0",
            "",
            "s",
        ]
        data = await self.__request(LOGIN_URL, data=payload)
        if data["success"] != "true":
            await self.__session.close()
            self.__session = None
            raise LoginException(user)
        return True

    async def measurement(self) -> dict:
        """Returns a measurement from the powermeter."""
        data = await self.__request(WATTHOURMETER_URL)
        return {
            "id": data["codSolicitudTGT"],
            "meter": data["valLecturaContador"],
            "consumption": data["valMagnitud"],
            "icp": data["valInterruptor"],
            "raw_response": data,
        }

    async def current_kilowatt_hour_read(self) -> float:
        """Returns the current read of the electricity meter."""
        return (await self.measurement())["meter"]

    async def current_power_consumption(self) -> float:
        """Returns your current power consumption."""
        return (await self.measurement())["consumption"]

    @deprecated("Use 'current_power_consumption' method instead")
    async def watthourmeter(self) -> float:
        """Returns your current power consumption."""
        return (await self.measurement())["consumption"]

    async def icpstatus(self) -> bool:
        """Returns the status of your ICP."""
        data = await self.__request(ICP_STATUS_URL, data="")
        return data["icp"] == "trueConectado"

    async def contracts(self) -> Optional[dict]:
        data = await self.__request(CONTRACTS_URL)
        if data["success"]:
            return data["contratos"]
        return None

    async def contract(self) -> dict:
        return await self.__request(CONTRACT_DETAIL_URL)

    async def contractselect(self, id: str) -> bool:
        data = await self.__request(CONTRACT_SELECTION_URL + id)
        if not data["success"]:
            raise SelectContractException
        return True

    async def scene_list(self) -> dict:
        data = await self.__request(OBTENER_ESCENARIOS_URL)
        return {"scene_names": data["y"]["smps"], "raw_response": data}

    async def scene_get(self, name: str) -> dict:
        data = await self.__request(OBTENER_ESCENARIO_URL, data={"nomEscenario": name})
        return {
            "name": data["nomEscenario"],
            "description": data["descripcion"],
            "consumption": data["numLcaInsta"],
            "raw_response": data,
        }

    async def scene_save(
        self, consumption: str, measurement_id: str, description: str
    ) -> dict:
        data = await self.__request(
            GUARDAR_ESCENARIO_URL.format(consumption, measurement_id),
            data={
                "nomEscenario": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "descripcion": description,
            },
        )
        return {"name": data["nomEscenario"], "raw_response": data}

    async def scene_delete(self, name: str) -> bool:
        await self.__request(BORRAR_ESCENARIO_URL, data={"nomEscenario": name})
        return True

    async def _consumption_raw(self, start: datetime, end: datetime) -> list:
        return await self.__request(
            OBTENER_PERIODO_URL.format(
                start.strftime("%d-%m-%Y"), end.strftime("%d-%m-%Y")
            )
        )
    
    # [os]Get consumption data from a time period
    async def consumption_raw(self, start: datetime, end: datetime) -> list:
        return await self.__request(
            OBTENER_PERIODO_URL.format(
                start.strftime("%d-%m-%Y"), end.strftime("%d-%m-%Y")
            )
        )

    # Get consumption data from a time period
    #
    # start/end: datetime.date objects indicating the time period (both inclusive)
    # The supported time range seems to be (not documented) from Jan 1 in the previous year and a max
    # length of one year.
    #
    # Returns a list of consumptions starting a midnight on the start day until 23:00 on the last day.
    # Each value is the hourly consumption in Wh.
    async def consumption(self, start: datetime, end: datetime) -> list:
        data = await self._consumption_raw(start, end)
        return [float(x["valor"]) for x in data["y"]["data"][0] if x]
    



    
    async def consumption_hour(self, dataj) -> list:
        hour_kwh = []
        for x in dataj["y"]["data"][0]:
            if x:
                hour_kwh.append(float(x["valor"]))
        return hour_kwh




    async def _production_raw(self, start: datetime, end: datetime) -> list:
        return await self.__request(
            OBTENER_PERIODO_GENERACION_URL.format(
                start.strftime("%d-%m-%Y"), end.strftime("%d-%m-%Y")
            )
        )

    # Get production data from a time period
    #
    # start/end: datetime.date objects indicating the time period (both inclusive)
    # The supported time range seems to be (not documented) from Jan 1 in the previous year and a max
    # length of one year.
    #
    # Returns a list of production starting a midnight on the start day until 23:00 on the last day.
    # Each value is the hourly production in Wh.
    async def production(self, start: datetime, end: datetime) -> list:
        data = await self._production_raw(start, end)
        return [float(x["valor"]) for x in data["y"]["data"][0] if x]

    # Get total consumption in Wh (Watt-hour) over a time period
    #
    # start/end: datetime.date objects indicating the time period (both inclusive)
    async def total_consumption(self, start, end) -> float:
        data = await self._consumption_raw(start, end)
        return float(data["acumulado"])

# [os] +++++++++++++++++++++++++++++++++++++
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

# [os] +++++++++++++++++++++++++++++++++++++
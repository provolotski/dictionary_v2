import datetime
import logging
from fastapi.responses import JSONResponse
from fastapi import APIRouter
from schemas import DictionaryOut, DictionaryIn, Attribute
import models.dictionary as eisgs_dict
from typing import Optional

from config import LOG_FILE, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)

dict_router = APIRouter(prefix="/models", tags=["Dictionary"])


@dict_router.post("/newDictionary", response_model=DictionaryIn)
async def create_new_dictionary(dictionary: DictionaryIn):
    """
    Создаем новый справочник
    :param dictionary:
    :return:
    """
    logger.debug('создаем новый справочник')
    await eisgs_dict.create_new_dictionary(dictionary)
    return JSONResponse(content={"message": ' errmessage'}, status_code=200)


@dict_router.get(path='/list', response_model=list[DictionaryOut])
async def list_dictionaries():
    """
    Получение перечня всех справочников
    :return: набор справочников
    """
    logger.debug('получаем список справочников')
    return await eisgs_dict.get_dictionaries()


@dict_router.get(path='/structure/', response_model=list[Attribute])
@dict_router.post(path='/structure/', response_model=list[Attribute])
async def get_dictionary_structure(dictionary: int):
    """
     Получение структуры справочника
     :return: набор справочников
     """
    logger.debug('get получаем структуру справочника')
    return await eisgs_dict.get_dictionary_structure(dictionary)


@dict_router.get(path='/dictionary/')
@dict_router.post(path='/dictionary/')
async def get_dictionary(dictionary: int, date: Optional[datetime.date] = None):
    """
    Получение всех значения справочника
    :param date: дата на которую нужно получить справочник, если не заполнена - текущая
    :param dictionary: идентификатор справочника
    :return: справочник целиком по структуре
    """
    logger.debug('endpoint получения всех значения по справочнику')
    if date is None:
        date = datetime.date.today()

    return await eisgs_dict.get_dictionary_values(dictionary, date)


@dict_router.get(path='/dictionaryValueByCode/')
@dict_router.post(path='/dictionaryValueByCode/')
async def get_dictionary_value_by_code(dictionary: int, code: str,  date: Optional[datetime.date] = None):
    logger.debug(f'endpoint получение  значений по коду dictionary ={dictionary}, code={code}, date = {date}')
    if code is None:
        return JSONResponse(content='код не может быть пустым', status_code=404)
    if date is None:
        date = datetime.date.today()
    return await eisgs_dict.get_dictionary_position_by_code(dictionary, code, date)


@dict_router.get(path='/dictionaryValueByID')
@dict_router.post(path='/dictionaryValueByID')
async def get_dictionary_value_by_id(dictionary: int, position_id: int, date: Optional[datetime.date] = None):
    logger.debug(f'endpoint получение  значений по коду dictionary ={dictionary}, id={position_id}, date = {date}')
    if id is None:
        return JSONResponse(content='код не может быть пустым', status_code=404)
    if date is None:
        date = datetime.date.today()
    return await eisgs_dict.get_dictionary_position_by_id(dictionary, position_id, date)


@dict_router.get(path='/findDictionaryByName')
@dict_router.post(path='/findDictionaryByName')
async def find_dictionary_by_name(name: str):
    logger.debug(f'endpoint поиск справочника по имени {name} for')
    return await eisgs_dict.find_dictionary_by_name(name)


@dict_router.get(path='/findDictionaryValue')
@dict_router.post(path='/findDictionaryValue')
async def find_dictionary_value(dictionary: int, findstr: str):
    logger.debug(f'endpoint поиск значений справочника  по имени {findstr} в справочнике {dictionary}')
    return await eisgs_dict.find_dictionary_by_name(findstr)

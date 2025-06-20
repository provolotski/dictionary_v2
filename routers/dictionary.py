from datetime import date
import logging
import pandas as pd
from fastapi.responses import JSONResponse
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from schemas import DictionaryOut, DictionaryIn, AttributeIn, AttributeDict
import models.model_dictionary as eisgs_dict
from models.model_dictionary import DictionaryService
from typing import Optional
import io

from config import LOG_FILE, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)


def try_decode(content, encodings=None):
    if encodings is None:
        encodings = ['utf-8', 'windows-1251', 'cp1252', 'iso-8859-1']
    for enc in encodings:
        try:
            logger.debug(f' проверяем кодировку {enc}')
            return content.decode(enc)
        except UnicodeDecodeError:
            logger.error(f' кодировка {enc} не подошла')
            continue
    raise ValueError("Не удалось декодировать файл ни одной из кодировок")


dict_router = APIRouter(prefix="/models", tags=["Dictionary"])


@dict_router.post("/newDictionary", response_model=DictionaryIn)
async def create_new_dictionary(dictionary: DictionaryIn):
    """
    Создаем новый справочник
    :param dictionary:
    :return:
    """
    logger.debug('создаем новый справочник')
    logger.debug(f'dictionary: {dictionary}')
    logger.debug(dictionary.start_date)
    if dictionary.start_date <= date.today() <= dictionary.finish_date:
        dictionary.id_status = 1
    else:
        dictionary.id_status = 0
    await DictionaryService.create(dictionary)
    return JSONResponse(content={"message": ' errmessage'}, status_code=200)


@dict_router.get(path='/list', response_model=list[DictionaryOut])
async def list_dictionaries():
    """
    Получение перечня всех справочников
    :return: набор справочников
    """
    logger.debug('получаем список справочников')
    return await DictionaryService.get_all()


@dict_router.get(path='/structure/', response_model=list[AttributeIn])
@dict_router.post(path='/structure/', response_model=list[AttributeIn])
async def get_dictionary_structure(dictionary: int):
    """
     Получение структуры справочника
     :return: набор справочников
     """
    logger.debug('get получаем структуру справочника')
    return await DictionaryService.get_dictionary_structure(dictionary)


@dict_router.post(path='/add_attribute/', response_model=AttributeDict)
async def add_attribute(attribute: AttributeDict):
    """
     Добавление нового атрибута в справочник
    :param attribute: описание атрибута
    :return:
    """
    logger.debug('добавление атрибута в справочник')
    await  eisgs_dict.create_attr_in_dictionary(attribute)
    return JSONResponse(content={"message": ' errmessage'}, status_code=200)


@dict_router.get(path='/dictionary/')
@dict_router.post(path='/dictionary/')
async def get_dictionary(dictionary: int, date: date = Query(default_factory=date.today) ):
    """
    Получение всех значений справочника
    :param date: дата на которую нужно получить справочник, если не заполнена - текущая
    :param dictionary: идентификатор справочника
    :return: справочник целиком по структуре
    """
    logger.debug('endpoint получения всех значений справочника')
    return await DictionaryService.get_dictionary_values(dictionary, date)


@dict_router.get(path='/dictionaryValueByCode/')
@dict_router.post(path='/dictionaryValueByCode/')
async def get_dictionary_value_by_code(dictionary: int, code: str, date: date = Query(default_factory=date.today) ):
    logger.debug(f'endpoint получение  значений по коду dictionary ={dictionary}, code={code}, date = {date}')
    if code is None:
        return JSONResponse(content='код не может быть пустым', status_code=404)
    return await DictionaryService.get_dictionary_position_by_code(dictionary, code, date)


@dict_router.get(path='/dictionaryValueByID')
@dict_router.post(path='/dictionaryValueByID')
async def get_dictionary_value_by_id(dictionary: int, position_id: int, date: Optional[date] = None):
    logger.debug(f'endpoint получение  значений по коду dictionary ={dictionary}, id={position_id}, date = {date}')
    if id is None:
        return JSONResponse(content='код не может быть пустым', status_code=404)
    return await DictionaryService.get_dictionary_position_by_id(dictionary, position_id, date)


@dict_router.get(path='/findDictionaryByName')
@dict_router.post(path='/findDictionaryByName')
async def find_dictionary_by_name(name: str):
    logger.debug(f'endpoint поиск справочника по имени {name} for')
    return await DictionaryService.find_dictionary_by_name(name)


@dict_router.get(path='/findDictionaryValue')
@dict_router.post(path='/findDictionaryValue')
async def find_dictionary_value(dictionary: int, findstr: str):
    logger.debug(f'endpoint поиск значений справочника  по имени {findstr} в справочнике {dictionary}')
    return await DictionaryService.find_dictionary_by_name(findstr)


@dict_router.post(path='/importCSV')
async def import_csv(dictionary: int, file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Файл должен быть в формате CSV")
    try:
        contents = await file.read()
        decoded = try_decode(contents)
        df = pd.read_csv(io.StringIO(decoded), dtype=str)
        if await DictionaryService.insert_dictionary_values(dictionary, df):
            return JSONResponse(status_code=200, content={
                "message": "Файл успешно обработан"})
        else:
            raise HTTPException(status_code=400, detail="Ошибка обработки файла")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки файла: {str(e)}")


@dict_router.post(path='/genRel')
async def import_csv(dictionary: int):
    logger.info('start generate relations')
    await eisgs_dict.generate_relation(dictionary)
    logger.info('finish generate relations')
    return JSONResponse(status_code=200, content={
        "message": "Файл успешно обработан"})

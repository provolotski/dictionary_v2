"""
Endpoint для системы справочников
"""

import io
from datetime import date as datetime_date
import logging
from typing import Optional, List
import pandas as pd
from fastapi.responses import JSONResponse
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends

# pylint: disable=import-error
from models.model_attribute import AttributeManager
from models.model_dictionary import DictionaryService

from schemas import DictionaryOut, DictionaryIn, AttributeIn, AttributeDict, AttrShown


from config import LOG_FILE, LOG_LEVEL

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(name)-30s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def get_current_date():
    """
    Получение текущей даты
    :return:
    """
    return datetime_date.today()


async def get_upload_file(file: UploadFile = File(...)) -> UploadFile:  # noqa: B008
    """
    Заглушка для загрузки файла
    :param file:
    :return:
    """
    return file


def try_decode(content, encodings=None):
    """
    Декодирование файла
    """
    if encodings is None:
        encodings = ["utf-8", "windows-1251", "cp1252", "iso-8859-1"]
    for enc in encodings:
        try:
            logger.debug(" проверяем кодировку %s", enc)
            return content.decode(enc)
        except UnicodeDecodeError:
            logger.error(" кодировка %s  не подошла", enc)
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
    logger.debug("создаем новый справочник")
    logger.debug(dictionary.start_date)
    if dictionary.start_date <= datetime_date.today() <= dictionary.finish_date:
        dictionary.id_status = 1
    else:
        dictionary.id_status = 0
    await DictionaryService.create(dictionary)
    return JSONResponse(content={"message": " Справочник создан"}, status_code=200)


@dict_router.post("/EditDictionary", response_model=DictionaryIn)
async def edit_dictionary(dictionary_id: int, dictionary: DictionaryIn):
    """
    Редактируем справочник
    :param dictionary_id:
    :param dictionary:
    :dictionary_id: идентификатор справочника
    :return:
    """
    logger.debug("Редактируем справочник")
    logger.debug(dictionary.start_date)
    if dictionary.start_date <= datetime_date.today() <= dictionary.finish_date:
        dictionary.id_status = 1
    else:
        dictionary.id_status = 0
    await DictionaryService.update(dictionary_id, dictionary)
    return JSONResponse(content={"message": " все ок"}, status_code=200)


@dict_router.post("/CreatePosition")
async def create_position(dictionary_id: int, attrs: List[AttrShown]):
    """
    Редактируем справочник
    :param attrs:
    :param dictionary_id: идентификатор справочника
    :return:
    """
    logger.debug("Добавляем значение в справочник")
    logger.debug("dictionary: %d", dictionary_id)
    await AttributeManager.create_position(dictionary_id, attrs)
    return JSONResponse(content={"message": " все ок"}, status_code=200)


@dict_router.post("/EditPosition")
async def edit_position(position_id: int, attrs: List[AttrShown]):
    """
    Редактируем справочник
    :param position_id: Идентификатор позиции
    :param attrs: словарь новых значений
    :return:
    """
    logger.debug("Добавляем значение в справочник")
    logger.debug("position: %d", position_id)
    await AttributeManager.edit_position(position_id, attrs)
    return JSONResponse(content={"message": " все ок"}, status_code=200)


@dict_router.get(path="/list", response_model=list[DictionaryOut])
async def list_dictionaries():
    """
    Получение перечня всех справочников
    :return: набор справочников
    """
    logger.debug("получаем список справочников")
    return await DictionaryService.get_all()


@dict_router.get(path="/structure/", response_model=list[AttributeIn])
@dict_router.post(path="/structure/", response_model=list[AttributeIn])
async def get_dictionary_structure(dictionary: int):
    """
    Получение структуры справочника
    :return: набор справочников
    """
    logger.debug("get получаем структуру справочника")
    return await DictionaryService.get_dictionary_structure(dictionary)


@dict_router.post(path="/add_attribute/", response_model=AttributeDict)
async def add_attribute(attribute: AttributeDict):
    """
     Добавление нового атрибута в справочник
    :param attribute: описание атрибута
    :return:
    """
    logger.debug("добавление атрибута в справочник")
    await DictionaryService.create_attr_in_dictionary(attribute)
    return JSONResponse(content={"message": " Атрибут добавлен"}, status_code=200)


@dict_router.get(path="/dictionaryValueByCode/")
@dict_router.post(path="/dictionaryValueByCode/")
async def get_dictionary_value_by_code(
    dictionary: int,
    code: str,
    date: datetime_date = Depends(get_current_date),  # noqa: B008
):
    """
    Получение значений по коду
    :param dictionary:
    :param code:
    :param date:
    :return:
    """

    logger.debug(
        "endpoint получение  значений по коду dictionary = %d, code=%s, date = %s",
        dictionary,
        code,
        str(date),
    )
    if code is None:
        return JSONResponse(content="код не может быть пустым", status_code=404)
    return await DictionaryService.get_dictionary_position_by_code(
        dictionary, code, date
    )


@dict_router.get(path="/dictionaryValueByID")
@dict_router.post(path="/dictionaryValueByID")
async def get_dictionary_value_by_id(
    dictionary: int, position_id: int, date: Optional[datetime_date] = None
):
    """
    Получение значений по идентификатору
    :param dictionary:
    :param position_id:
    :param date:
    :return:
    """

    logger.debug(
        "endpoint получение значений по коду dictionary =%d, id=%d, date = %s",
        dictionary,
        position_id,
        str(date),
    )
    if id is None:
        return JSONResponse(content="код не может быть пустым", status_code=404)
    return await DictionaryService.get_dictionary_position_by_id(
        dictionary, position_id, date
    )


@dict_router.get(path="/findDictionaryByName")
@dict_router.post(path="/findDictionaryByName")
async def find_dictionary_by_name(name: str):
    """
    Поиск справочника по имени
    :param name:
    :return:
    """
    logger.debug("endpoint поиск справочника по имени %s", name)
    return await DictionaryService.find_dictionary_by_name(name)


@dict_router.get(path="/findDictionaryValue")
@dict_router.post(path="/findDictionaryValue")
async def find_dictionary_value(
    dictionary: int, findstr: str, date: Optional[datetime_date] = None
):
    """
     Поиск значений справочника по имени
    :param dictionary:
    :param findstr:
    :param date:
    :return:
    """
    logger.debug(
        "endpoint поиск значений справочника  по имени %s  в справочнике %s ",
        findstr,
        dictionary,
    )
    return await DictionaryService.find_dictionary_position_by_expression(
        dictionary, findstr, date
    )


@dict_router.post(path="/importCSV")
async def import_csv(
    dictionary: int, file: UploadFile = Depends(get_upload_file)  # noqa: B008
):
    """
    Загрузка из CSV
    :param dictionary:
    :param file:
    :return:
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Файл должен быть в формате CSV")
    try:
        contents = await file.read()
        decoded = try_decode(contents)
        df = pd.read_csv(io.StringIO(decoded), dtype=str)
        if await DictionaryService.insert_dictionary_values(dictionary, df):
            return JSONResponse(
                status_code=200, content={"message": "Файл успешно обработан"}
            )
    except Exception as e:
        logger.error(str(e))
    raise HTTPException(status_code=400, detail="Ошибка обработки файла")


@dict_router.get(path="/dictionary/")
@dict_router.post(path="/dictionary/")
async def get_dictionary(
    dictionary: int, date: datetime_date = Depends(get_current_date)  # noqa: B008
):
    """..."""
    logger.debug("endpoint получения всех значений справочника")
    return await DictionaryService.get_dictionary_values(dictionary, date)

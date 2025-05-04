import datetime
import logging

import schemas
from database import database
from config import LOG_FILE, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)


async def get_dictionaries() -> list[schemas.DictionaryOut]:
    sql = "select id, name, code from dictionary"
    rows = await database.fetch_all(sql)
    return [schemas.DictionaryOut(**dict(row)) for row in rows]


async def create_new_dictionary(dictionary: schemas.DictionaryIn):
    logger.debug('creating new models')
    sql = 'insert into dictionary (name, code, description) values (:name, :code, :description) returning id'
    dic_id = await database.execute(sql, values=dictionary.model_dump())
    return dic_id


async def get_dictionary_structure(dictionary_id: int) -> list[schemas.Attribute]:
    logger.debug(f'получаем структуру справочника с id = {dictionary_id}')
    """
    # sql = "select * from dictionary_attribute where id_dictionary =:id"
    # rows = await database.fetch_all(sql,values={"id":dictionary_id})
    # return [Attribute(**dict(row)) for row in rows]
    """
    if dictionary_id is not None:
        return [
            schemas.Attribute(id=1, name='Наименование', type='строка', start_date=datetime.date(1900, 1, 1),
                              end_date=datetime.date(9999, 12, 31),
                              required=True, capacity=56, alt_name='Name'),
            schemas.Attribute(id=1, name='Наименование ENG', type='строка', start_date=datetime.date(1900, 1, 1),
                              end_date=datetime.date(9999, 12, 31),
                              required=True, capacity=56, alt_name='Name_eng')
        ]


async def get_dictionary_values(dictionary_id: int, date: datetime.date) -> list[schemas.DictionaryPosition]:
    """
    Получение справочника целиком
    :param dictionary_id:
    :param date:
    :return:
    """
    logger.debug(f'получение всех значений справочника с id ={dictionary_id}  на дату {date}')
    return [
        schemas.DictionaryPosition(id=1, parent_id=0, parent_code='',
                                   attr=schemas.ListAttr(attrs=schemas.AttrShown(name='Название', value='Итого')))
    ]


async def get_dictionary_position_by_code(dictionary_id: int, code: str, date: datetime.date) -> list[
    schemas.DictionaryPosition]:
    """
    Получение позиции справочника по коду
    :param dictionary_id:
    :param code:
    :param date:
    :return:
    """
    logger.debug(f'получение позиции справочника с id = {dictionary_id} по коду {code} на дату {date}')
    return [
        schemas.DictionaryPosition(id=2, parent_id=1, parent_code='100000000',
                                   attr=schemas.ListAttr(attrs=schemas.AttrShown(name='Название', value='Итого')))
    ]


async def get_dictionary_position_by_id(dictionary: int, id: int, date: datetime.date) -> schemas.DictionaryPosition:
    """
    Получение позиции справочника по id
    :param dictionary: 
    :param id: 
    :param date: 
    :return: 
    """
    logger.debug(f'Получение позиции справочника по ID = {id} из справочника {dictionary} на дату {date}')
    return schemas.DictionaryPosition(id=3, parent_id=1, parent_code='100000000',
                               attr=schemas.ListAttr(attrs=schemas.AttrShown(name='Название', value='Итого')))

async def find_dictionary_by_name (name:str) ->list[schemas.DictionaryOut]:
    """
    Поиск справочника по имени
    :param name:
    :return:
    """
    logger.debug(f'поиск справочника по имени поисковая строка:{name}')
    return [
        schemas.DictionaryOut(id=1,name='справочник 1',code='asdasd', description='asdasdasdasd'),
        schemas.DictionaryOut(id=2, name='справочник 1', code='asdasd', description='asdasdasdasd')
    ]

async def find_dictionary_position_by_expression(dictionary:int, find_str:str)->list[schemas.DictionaryPosition]:
    logger.debug(f'поиск значений справочника по  поисковая строка:{find_str} в справочнике {dictionary}')
    return [
        schemas.DictionaryPosition(id=2, parent_id=1, parent_code='100000000',
                                   attr=schemas.ListAttr(attrs=schemas.AttrShown(name='Название', value='Итого')))
    ]
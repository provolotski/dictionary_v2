import datetime
import logging

import schemas
from database import database
from config import LOG_FILE, LOG_LEVEL
import models.model_attribute as da


logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)


async def get_dictionaries() -> list[schemas.DictionaryOut]:
    sql = ("select id, name, code, description,start_date, finish_date, name_eng, name_bel,"
           "description_eng, description_bel, gko, organization,classifier,id_status, id_type  from dictionary")
    rows = await database.fetch_all(sql)
    return [schemas.DictionaryOut(**dict(row)) for row in rows]


async def create_new_dictionary(dictionary: schemas.DictionaryIn):
    logger.debug('creating new models')
    # Создаем справочник
    sql = ('insert into dictionary (name, code, description,start_date, finish_date, '
           'change_date, name_eng, name_bel,description_eng, description_bel, gko,'
           'organization,classifier,id_status, id_type) '
           'values (:name, :code, :description, :start_date, :finish_date, '
           'current_date, :name_eng, :name_bel, :description_eng, :description_bel, :gko,'
           ':organization, :classifier,:id_status,:id_type) returning id')
    dic_id = await database.execute(sql, values=dictionary.model_dump())

    # Создаем обязательные поля
    # Поле наименование

    attr = schemas.AttributeDict(
        name="Наименование",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="NAME",
        id_dictionary=dic_id
    )
    attr_id = await create_attr_in_dictionary(attr)
    # Код наименование
    attr = schemas.AttributeDict(
        name="Код",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="CODE",
        id_dictionary=dic_id
    )

    attr_id = await create_attr_in_dictionary(attr)
    # Код наименование
    attr = schemas.AttributeDict(
        name="Код родительской позиции",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="PARENT_CODE",
        id_dictionary=dic_id
    )

    attr_id = await create_attr_in_dictionary(attr)

    # Признак полноты
    attr = schemas.AttributeDict(
        name="Признак полноты итога",
        type=3,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="FULL_SUM",
        id_dictionary=dic_id
    )

    attr_id = await create_attr_in_dictionary(attr)

    # Признак полноты
    attr = schemas.AttributeDict(
        name="Дата начала действия позиции",
        type=3,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="START_DATE",
        id_dictionary=dic_id
    )

    attr_id = await create_attr_in_dictionary(attr)

    # Признак полноты
    attr = schemas.AttributeDict(
        name="Дата окончания действия позиции",
        type=3,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="FINISH_DATE",
        id_dictionary=dic_id
    )

    attr_id = await create_attr_in_dictionary(attr)

    # Поле наименование

    attr = schemas.AttributeDict(
        name="Наименование на белорусском языке",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="NAME_BEL",
        id_dictionary=dic_id
    )
    attr_id = await create_attr_in_dictionary(attr)

    # Поле наименование

    attr = schemas.AttributeDict(
        name="Наименование на английском языке",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=True,
        capacity=250,
        alt_name="NAME_ENG",
        id_dictionary=dic_id
    )
    attr_id = await create_attr_in_dictionary(attr)
    # Описание
    attr = schemas.AttributeDict(
        name="Описание",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=False,
        capacity=250,
        alt_name="Descr",
        id_dictionary=dic_id
    )

    attr_id = await create_attr_in_dictionary(attr)

    # Описание
    attr = schemas.AttributeDict(
        name="Описание на белорусском языке",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=False,
        capacity=250,
        alt_name="Descr_BEL",
        id_dictionary=dic_id
    )

    attr_id = await create_attr_in_dictionary(attr)

    # Описание
    attr = schemas.AttributeDict(
        name="Описание на английском языке",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=False,
        capacity=250,
        alt_name="Descr_ENG",
        id_dictionary=dic_id
    )
    attr_id = await create_attr_in_dictionary(attr)

    # Описание
    attr = schemas.AttributeDict(
        name="Комментарий",
        type=1,
        start_date=dictionary.start_date,
        finish_date=dictionary.finish_date,
        required=False,
        capacity=250,
        alt_name="COMMENT",
        id_dictionary=dic_id
    )
    attr_id = await create_attr_in_dictionary(attr)
    return dic_id


async def create_attr_in_dictionary(attribute: schemas.AttributeDict):
    logger.debug('create new attribute')
    sql = ('insert into dictionary_attribute '
           '(id_dictionary, name, required, start_date, finish_date,  alt_name,id_attribute_type,capacity) '
           'values '
           '(:id_dictionary, :name, :required, :start_date, :finish_date, :alt_name, :type, :capacity) '
           'returning id')
    attr_id = await database.execute(sql, values=attribute.model_dump())


async def get_dictionary_structure(dictionary_id: int) -> list[schemas.AttributeIn]:
    logger.debug(f'получаем структуру справочника с id = {dictionary_id}')
    sql = 'select id, name, id_attribute_type, start_date,finish_date,required,capacity, alt_name  from dictionary_attribute where id_dictionary =:id'
    rows = await database.fetch_all(sql,values={"id":dictionary_id})
    return [schemas.AttributeIn(**dict(row)) for row in rows]

    # if dictionary_id is not None:
    #     return [
    #         schemas.Attribute(id=1, name='Наименование', type='строка', start_date=datetime.date(1900, 1, 1),
    #                           end_date=datetime.date(9999, 12, 31),
    #                           required=True, capacity=56, alt_name='Name'),
    #         schemas.Attribute(id=1, name='Наименование ENG', type='строка', start_date=datetime.date(1900, 1, 1),
    #                           end_date=datetime.date(9999, 12, 31),
    #                           required=True, capacity=56, alt_name='Name_eng')
    #     ]


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


async def find_dictionary_by_name(name: str) -> list[schemas.DictionaryOut]:
    """
    Поиск справочника по имени
    :param name:
    :return:
    """
    logger.debug(f'поиск справочника по имени поисковая строка:{name}')
    return [
        schemas.DictionaryOut(id=1, name='справочник 1', code='asdasd', description='asdasdasdasd'),
        schemas.DictionaryOut(id=2, name='справочник 1', code='asdasd', description='asdasdasdasd')
    ]


async def find_dictionary_position_by_expression(dictionary: int, find_str: str) -> list[schemas.DictionaryPosition]:
    logger.debug(f'поиск значений справочника по  поисковая строка:{find_str} в справочнике {dictionary}')
    return [
        schemas.DictionaryPosition(id=2, parent_id=1, parent_code='100000000',
                                   attr=schemas.ListAttr(attrs=schemas.AttrShown(name='Название', value='Итого')))
    ]

async def insert_dictionary_values(id_dictionary: int, dataframe) -> True|False:
    try:
        await da.insert_new_values(id_dictionary, dataframe)
        return True
    except Exception as e:
        logger.error(e)
        return False
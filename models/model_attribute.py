import logging
from logging import raiseExceptions

import pandas as pd
from datetime import date
from database import database

from config import LOG_FILE, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)



async def get_start_date_dictionary(dictionary_id:int)->date:
    """
    Получаем дату начала действия справочника
    :param dictionary_id: идентификатор справочника
    :return: дата начала действия справочника
    """
    sql = 'select d.start_date from "dictionary" d where d.id =:id'
    row =  await database.fetch_one(sql, {'id': dictionary_id})
    return row['start_date']

async def get_finish_date_dictionary(dictionary_id:int)->date:
    """
      Получаем дату окончания действия справочника
      :param dictionary_id: идентификатор справочника
      :return: дата окончания действия справочника
      """
    sql = 'select d.finish_date from "dictionary" d where d.id =:id'
    row =  await database.fetch_one(sql, {'id': dictionary_id})
    return row['finish_date']

async def create_new_positions(dictionary_id:int)->int:
    """
    Создание новой позиции в справочнике
    :param dictionary_id: идентификатор справочника
    :return: идетнификатор созданной позиции
    """
    sql = 'insert into dictionary_positions (id_dictionary) values (:id_dictionary) returning id'
    row = await database.fetch_one(sql, {'id_dictionary': dictionary_id})
    return row['id']


async def get_attribute_list(dictionary_id:int)->list:
    """
     Получение атрибутов справочника
    :param dictionary_id: идентификатор справочника
    :return: list текстовых наименований Alt_names
    """
    sql = 'select d.alt_name  from dictionary_attribute d where d.id_dictionary =:id_dictionary and d.alt_name is not null'
    rows = await database.fetch_all(sql, values={"id_dictionary": dictionary_id})
    return [row[0] for row in rows]


async def get_parent_id(dictionary_id:int, parent_code:str)->int:
    """
    Поиск родительской позиции в справочнике
    :param dictionary_id: идентификатор справочника
    :param parent_code: код родительской позиции
    :return:
    """
    None

async def get_attribute_id(dictionary_id:int, attribute_name:str)->int:
    """
    получаем идентификатор атрибута по alt имени и идентификатору справочника
    :param dictionary_id:
    :param attribute_name:
    :return:
    """
    sql = 'select id from dictionary_attribute da  where da.id_dictionary =:id_dictionary and da.alt_name =:alt_name'
    row = await database.fetch_one(sql, {'id_dictionary': dictionary_id, 'alt_name': attribute_name})
    return row['id']



async def insert_attr_data(dictionary_id:int, position_id:int, attribute_name:str, value:str, start_date:date, finish_date:date):
    """
    вставляем значение в справочник
    :param dictionary_id:
    :param position_id:
    :param attribute_name:
    :param value:
    :param start_date:
    :param finish_date:
    :return:
    """
    attribute_id = await get_attribute_id(dictionary_id, attribute_name)
    logger.debug(f'attribute_id: {attribute_id}')

    # Обработка NaN
    clean_value = None if str(value).strip().lower() in ('nan', 'none', 'null', '') else value

    sql ='insert into dictionary_data (id_position, id_attribute, start_date, finish_date, value) values (:id_position, :id_attribute, :start_date, :finish_date, :value)'
    await database.execute(sql,{'id_position':position_id, 'id_attribute':attribute_id, 'start_date':start_date, 'finish_date':finish_date, 'value':clean_value})
    logger.debug(f'вставил значение {value} для атрибута {attribute_name}')


async def generate_relations_for_dictionary(dictionary_id:int):
    None


async def insert_new_values(dictionary_id:int, df: pd.DataFrame):
    """
    импорт значений справочника из pandas dataframe
    :param dictionary_id: идентификатор справочника
    :param df: импортированный dataframe
    :return:
    """
    start_date = await get_start_date_dictionary(dictionary_id)
    finish_date = await get_finish_date_dictionary(dictionary_id)
    cols = await get_attribute_list(dictionary_id)
    if 'CODE' in df.columns and 'NAME' in df.columns:
        for index, row in df.iterrows():
            if not pd.isna(row['CODE']) and not pd.isna(row['NAME']):
                position_id = await create_new_positions(dictionary_id)
                for col in cols:
                    if col in row:
                        # if not pd.isna(row[col]):
                        logger.debug(f'нашел значение для {col}')
                        try:
                            await insert_attr_data(dictionary_id, position_id, col, str(row[col]), start_date, finish_date)
                        except Exception as e:
                            logger.error(f'При добавлении значения в колонку {col}  значения {row[col]} произошла ошибка {str(e)}')
    else:
        logger.error('Не найдены поля NAME или CODE')
        raise Exception("'Не найдены поля NAME или CODE")



            # if 'PARENT_CODE' in row:
            #     if pd.isna(row['PARENT_CODE']):
            #         logger.debug(f'нет Parent Code для {row["NAME"]}')
            #
            #     else:
            #         logger.debug(f'Parent Code для {row["PARENT_CODE"]}')
            #         logger.debug(f'есть Parent Code для {row["NAME"]}')




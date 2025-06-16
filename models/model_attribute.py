import logging

import pandas as pd
from datetime import date
from database import database

from config import LOG_FILE, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)


async def get_start_date_dictionary(dictionary_id: int) -> date:
    """
    Получаем дату начала действия справочника
    :param dictionary_id: идентификатор справочника
    :return: дата начала действия справочника
    """
    sql = 'select d.start_date from dictionary d where d.id =:id'
    row = await database.fetch_one(sql, {'id': dictionary_id})
    return row['start_date']


async def get_finish_date_dictionary(dictionary_id: int) -> date:
    """
      Получаем дату окончания действия справочника
      :param dictionary_id: идентификатор справочника
      :return: дата окончания действия справочника
      """
    sql = 'select d.finish_date from dictionary d where d.id =:id'
    row = await database.fetch_one(sql, {'id': dictionary_id})
    return row['finish_date']


async def create_new_positions(dictionary_id: int) -> int:
    """
    Создание новой позиции в справочнике
    :param dictionary_id: идентификатор справочника
    :return: идентификатор созданной позиции
    """
    sql = 'insert into dictionary_positions (id_dictionary) values (:id_dictionary) returning id'
    row = await database.fetch_one(sql, {'id_dictionary': dictionary_id})
    return row['id']


async def get_attribute_list(dictionary_id: int) -> list:
    """
     Получение атрибутов справочника
    :param dictionary_id: идентификатор справочника
    :return: list текстовых наименований Alt_names
    """
    sql = ('select d.alt_name  from dictionary_attribute d where d.id_dictionary =:id_dictionary and d.alt_name is not '
           'null')
    rows = await database.fetch_all(sql, values={"id_dictionary": dictionary_id})
    return [row[0] for row in rows]


async def get_attribute_id(dictionary_id: int, attribute_name: str) -> int:
    """
    Получаем идентификатор атрибута по alt имени и идентификатору справочника
    :param dictionary_id:
    :param attribute_name:
    :return:
    """
    sql = 'select id from dictionary_attribute da  where da.id_dictionary =:id_dictionary and da.alt_name =:alt_name'
    row = await database.fetch_one(sql, {'id_dictionary': dictionary_id, 'alt_name': attribute_name})
    return row['id']


async def insert_attr_data(dictionary_id: int, position_id: int, attribute_name: str, value: str, start_date: date,
                           finish_date: date):
    """
    Вставляем значение в справочник
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

    sql = ('insert into dictionary_data (id_position, id_attribute, start_date, finish_date, value) values ('
           ':id_position, :id_attribute, :start_date, :finish_date, :value)')
    await database.execute(sql, {'id_position': position_id, 'id_attribute': attribute_id, 'start_date': start_date,
                                 'finish_date': finish_date, 'value': clean_value})
    logger.debug(f'вставил значение {value} для атрибута {attribute_name}')


async def generate_relations_for_dictionary(dictionary_id: int):
    """
    Расставляем иерархию для справочника
    :param dictionary_id:  идентификатор справочника
    :return:
    """
    sql = "select id from dictionary_positions dp  where dp.id_dictionary =:id_dictionary"
    rows = await database.fetch_all(sql, {'id_dictionary': dictionary_id})
    for row in rows:
        await update_relation_for_positions(row[0], dictionary_id)


def is_valid_dates(row, row_attr) -> bool:
    return row_attr['start_date'] < row['finish_date'] and row_attr['finish_date'] > row['start_date']


async def update_relation_for_positions(position_id: int, dictionary_id: int):
    sql = "delete from dictionary_relations where id_positions = :id"
    await database.execute(sql, {'id': position_id})
    sql = ("select dd.value, dd.start_date, dd.finish_date from dictionary_data dd, dictionary_attribute da   where "
           "dd.id_position =:id_position and dd.id_attribute =da.id and da.alt_name ='PARENT_CODE'")
    rows = await database.fetch_all(sql, {'id_position': position_id})
    for row in rows:
        sql_attr = ("select dd.id_position, dd.start_date, dd.finish_date  from dictionary_data dd, "
                    "dictionary_attribute da   where dd.id_attribute =da.id and da.id_dictionary =:id_dictionary and "
                    "da.alt_name ='CODE' and dd.value =cast (:parent_code as text) order by 2")
        rows_attr = await database.fetch_all(sql_attr, {'id_dictionary': dictionary_id, 'parent_code': row['value']})
        for row_attr in rows_attr:
            if is_valid_dates(row, row_attr):
                start_date = max(row_attr['start_date'], row['start_date'])
                finish_date = min(row_attr['finish_date'], row['finish_date'])
                sql_ins = ('insert into dictionary_relations (id_positions, id_parent_positions, start_date, '
                           'finish_date) values(:id_positions, :id_parent_positions, :start_date, :finish_date)')
                try:
                    await database.execute(sql_ins, {'id_positions': position_id, 'id_parent_positions': row_attr[0],
                                                     'start_date': start_date, 'finish_date': finish_date})
                    logger.info(
                        f'вставили успешно id_positions: {position_id}, id_parent_positions:{row_attr[0]}, start_date:'
                        f'{start_date}, finish_date:{finish_date}')
                except Exception as e:
                    logger.error(
                        f'Ошибка {str(e)} вставки id_positions: {position_id}, id_parent_positions:{row_attr[0]}, '
                        f'start_date:{start_date}, finish_date:{finish_date}')


async def insert_new_values(dictionary_id: int, df: pd.DataFrame):
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
                        logger.debug(f'нашел значение для {col}')
                        try:
                            await insert_attr_data(dictionary_id, position_id, col, str(row[col]), start_date,
                                                   finish_date)
                        except Exception as e:
                            logger.error(
                                f'При добавлении значения в колонку {col}  значения {row[col]} '
                                f'произошла ошибка {str(e)}')
    else:
        logger.error('Не найдены поля NAME или CODE')
        raise ValueError('Не найдены поля NAME или CODE')

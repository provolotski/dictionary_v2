import datetime
import logging
import traceback

from typing import List, Optional

import pandas as pd
from pydantic import BaseModel

import schemas
from database import database
from config import LOG_FILE, LOG_LEVEL
from  models.model_attribute import AttributeManager


logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)


class DictionaryService:
    # константы
    DEFAULT_CAPACITY = 250

    REQUIRED_ATTRIBUTES = [
        {"name": "Наименование", "alt_name": "NAME", "required": True, "id_attribute_type": 0},
        {"name": "Код", "alt_name": "CODE", "required": True, "id_attribute_type": 0},
        {"name": "Код родительской позиции", "alt_name": "PARENT_CODE", "required": True, "id_attribute_type": 0},
        {"name": "Признак полноты итога", "alt_name": "FULL_SUM", "required": True, "id_attribute_type": 3},
        {"name": "Дата начала действия позиции", "alt_name": "START_DATE", "required": True, "id_attribute_type": 2},
        {"name": "Дата окончания действия позиции", "alt_name": "FINISH_DATE", "required": True,
         "id_attribute_type": 2},
        {"name": "Наименование на белорусском языке", "alt_name": "NAME_BEL", "required": True, "id_attribute_type": 0},
        {"name": "Наименование на английском языке", "alt_name": "NAME_ENG", "required": True, "id_attribute_type": 0},
        {"name": "Описание", "alt_name": "Descr", "required": False, "id_attribute_type": 0},
        {"name": "Описание на белорусском языке", "alt_name": "Descr_BEL", "required": False, "id_attribute_type": 0},
        {"name": "Описание на английском языке", "alt_name": "Descr_ENG", "required": False, "id_attribute_type": 0},
        {"name": "Комментарий", "alt_name": "COMMENT", "required": False, "id_attribute_type": 0},
    ]

    @staticmethod
    async def get_all() -> List[schemas.DictionaryOut]:
        """Получение всех справочников"""

        sql = ("select id, name, code, description,start_date, finish_date, name_eng, name_bel, description_eng, "
               "description_bel, gko, organization,classifier,id_status, id_type  from dictionary")
        rows = await database.fetch_all(sql)
        return [schemas.DictionaryOut(**dict(row)) for row in rows]

    @staticmethod
    async def find_dictionary_by_name(name: str) -> List[schemas.DictionaryOut]:
        """
        Поиск справочника по имени
        :param name:
        :return:
        """
        logger.debug(f'поиск справочника по имени поисковая строка:{name}')
        sql = ("select id, name, code, description,start_date, finish_date, name_eng, name_bel, description_eng, "
               "description_bel, gko, organization,classifier,id_status, id_type  from dictionary where name like "
               "'%'||:name||'%'")
        rows = await database.fetch_all(sql, {'name': name})
        return [schemas.DictionaryOut(**dict(row)) for row in rows]

    @staticmethod
    async def create(dictionary: schemas.DictionaryIn) -> int:
        """
        Создание справочника
        :param dictionary:
        :return:
        """
        sql = ('insert into dictionary (name, code, description,start_date, finish_date, change_date, name_eng, '
               'name_bel,description_eng, description_bel, gko, organization,classifier,id_status, id_type) values ('
               ':name, :code, :description, :start_date, :finish_date, current_date, :name_eng, :name_bel, '
               ':description_eng, :description_bel, :gko, :organization, :classifier,:id_status,:id_type) returning id')
        dict_id = await database.execute(sql, values=dictionary.model_dump())

        # Создаем обязательные параметры

        for attr_config in DictionaryService.REQUIRED_ATTRIBUTES:
            attr = schemas.AttributeDict(
                id_dictionary = dict_id,
                start_date = dictionary.start_date,
                finish_date = dictionary.finish_date,
                capacity =DictionaryService.DEFAULT_CAPACITY,
                **attr_config
            )
            await DictionaryService._create_attribute(attr)

        logger.info(f"Created dictionary ID: {dict_id}")
        return dict_id

    @staticmethod
    async def _create_attribute(attribute: schemas.AttributeDict) -> int:
        sql = """
            INSERT INTO dictionary_attribute (
                id_dictionary, name, required, start_date, 
                finish_date, alt_name, id_attribute_type, capacity
            ) VALUES (
                :id_dictionary, :name, :required, :start_date,
                :finish_date, :alt_name, :id_attribute_type, :capacity
            ) RETURNING id
        """
        try:
            return await database.execute(sql, values=attribute.model_dump())
        except Exception as e:
            logger.error(e)
            logger.error(attribute.model_dump())

    @staticmethod
    async def insert_dictionary_values(id_dictionary: int, dataframe) -> True | False:
        try:
            await AttributeManager.import_data(id_dictionary, dataframe)
            return True
        except Exception as e:
            logger.error(e)
            return False

    @staticmethod
    async def get_dictionary_values(dictionary_id: int, date: datetime.date) -> list[schemas.DictionaryPosition]:
        """
        Получение справочника целиком
        :param dictionary_id:
        :param date:
        :return:
        """
        logger.debug(f'получение всех значений справочника с id ={dictionary_id}  на дату {date}')

        sql = """
        WITH position_data AS (
 select
 	dp.id,
    t1.id_parent_positions AS parent_id,
    t1.value AS parent_code,
    dp.id_dictionary
            FROM dictionary_positions dp
            left join
            ( select dr.id_positions, dr.id_parent_positions,dd1.value
            from  dictionary_relations dr 
            JOIN dictionary_data dd1 ON dd1.id_position = dr.id_positions
            JOIN dictionary_attribute da1 ON dd1.id_attribute = da1.id AND da1.alt_name = 'PARENT_CODE' 
            where  :dt between dr.start_date and dr.finish_date and  :dt  between dd1.start_date and dd1.finish_date
            ) t1 on (dp.id = t1.id_positions)
            WHERE dp.id_dictionary = :id_dictionary
   ),
        attributes AS (
   select  pd.id,
                pd.parent_id,
                pd.parent_code,
                da.name AS attr_name,
                dd.value AS attr_value from position_data pd
   join dictionary_attribute da  on pd.id_dictionary =da.id_dictionary
   left outer join dictionary_data dd on (dd.id_position =pd.id and dd.id_attribute =da.id )
   where  :dt between dd.start_date and dd.finish_date
         )
        SELECT
            id,
            parent_id,
            parent_code,
            json_agg(
                json_build_object('name', attr_name, 'value', attr_value)
            ) AS attrs
        FROM attributes
        GROUP BY id, parent_id, parent_code
        ORDER BY id
        """
        rows = await database.fetch_all(sql, {'id_dictionary': dictionary_id})
        return [schemas.DictionaryPosition(**dict(row)) for row in rows]

    @staticmethod
    async def get_dictionary_structure(dictionary_id: int) -> list[schemas.AttributeIn]:
        logger.debug(f'получаем структуру справочника с id = {dictionary_id}')
        sql = ('select id, name, id_attribute_type, start_date,finish_date,required,capacity, alt_name  from '
               'dictionary_attribute where id_dictionary =:id')
        rows = await database.fetch_all(sql, values={"id": dictionary_id})
        return [schemas.AttributeIn(**dict(row)) for row in rows]

    @staticmethod
    async def create_attr_in_dictionary(attribute: schemas.AttributeDict):
        logger.debug('create new attribute')
        await DictionaryService._create_attribute(attribute)

    @staticmethod
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
        sql = """
                WITH position_data AS (
         select
         	dp.id,
            t1.id_parent_positions AS parent_id,
            t1.value AS parent_code,
            dp.id_dictionary
                    FROM dictionary_positions dp
                    left join
                    ( select dr.id_positions, dr.id_parent_positions,dd1.value
                    from  dictionary_relations dr 
                    JOIN dictionary_data dd1 ON dd1.id_position = dr.id_positions
                    JOIN dictionary_attribute da1 ON dd1.id_attribute = da1.id AND da1.alt_name = 'PARENT_CODE'
                    where  :dt between dr.start_date and dr.finish_date and  :dt  between dd1.start_date and dd1.finish_date
                    ) t1 on (dp.id = t1.id_positions)
                    WHERE dp.id_dictionary = :id_dictionary
                    and exists (select null from dictionary_data dd, dictionary_attribute da 
           where dd.id_attribute =da.id and da.alt_name ='CODE' and dd.value  like '%'||:code||'%' and dd.id_position =dp.id and :dt between dd.start_date and dd.finish_date)                    
           ),
                attributes AS (
           select  pd.id,
                        pd.parent_id,
                        pd.parent_code,
                        da.name AS attr_name,
                        dd.value AS attr_value from position_data pd
           join dictionary_attribute da  on pd.id_dictionary =da.id_dictionary
           left outer join dictionary_data dd on (dd.id_position =pd.id and dd.id_attribute =da.id )
           where  :dt between dd.start_date and dd.finish_date
                 )
                SELECT
                    id,
                    parent_id,
                    parent_code,
                    json_agg(
                        json_build_object('name', attr_name, 'value', attr_value)
                    ) AS attrs
                FROM attributes
                GROUP BY id, parent_id, parent_code
                ORDER BY id
                """
        rows = await database.fetch_all(sql, {'id_dictionary': dictionary_id, 'code': code, 'dt': date})
        return [schemas.DictionaryPosition(**dict(row)) for row in rows]

    @staticmethod
    async def get_dictionary_position_by_id(dictionary_id: int, id: int,
                                            date: datetime.date) -> schemas.DictionaryPosition:
        """
        Получение позиции справочника по id
        :param dictionary:
        :param id:
        :param date:
        :return:
        """
        logger.debug(f'Получение позиции справочника по ID = {id} из справочника {dictionary_id} на дату {date}')
        sql = """
                       WITH position_data AS (
                select
                	dp.id,
                   t1.id_parent_positions AS parent_id,
                   t1.value AS parent_code,
                   dp.id_dictionary
                           FROM dictionary_positions dp
                           left join
                           ( select dr.id_positions, dr.id_parent_positions,dd1.value
                           from  dictionary_relations dr 
                           JOIN dictionary_data dd1 ON dd1.id_position = dr.id_positions
                           JOIN dictionary_attribute da1 ON dd1.id_attribute = da1.id AND da1.alt_name = 'PARENT_CODE'
                           where :dt between dr.start_date and dr.finish_date and  :dt  between dd1.start_date and dd1.finish_date
                           ) t1 on (dp.id = t1.id_positions)
                           WHERE dp.id_dictionary = :id_dictionary
                        and dp.id = :id 

                  ),
                       attributes AS (
                  select  pd.id,
                               pd.parent_id,
                               pd.parent_code,
                               da.name AS attr_name,
                               dd.value AS attr_value from position_data pd
                  join dictionary_attribute da  on pd.id_dictionary =da.id_dictionary
                  left outer join dictionary_data dd on (dd.id_position =pd.id and dd.id_attribute =da.id )
                   where  :dt between dd.start_date and dd.finish_date
                        )
                       SELECT
                           id,
                           parent_id,
                           parent_code,
                           json_agg(
                               json_build_object('name', attr_name, 'value', attr_value)
                           ) AS attrs
                       FROM attributes
                       GROUP BY id, parent_id, parent_code
                       ORDER BY id;
                       """
        rows = await database.fetch_all(sql, {'id_dictionary': dictionary_id, 'id': id})
        return [schemas.DictionaryPosition(**dict(row)) for row in rows]

    @staticmethod
    async def find_dictionary_position_by_expression(dictionary_id: int, find_str: str, date: datetime.date) -> List[
        schemas.DictionaryPosition]:
        logger.debug(f'поиск значений справочника по  поисковая строка:{find_str} в справочнике {dictionary_id}')
        if date is None:
            date = datetime.date.today()
        sql = """
                       WITH position_data AS (
                select
                    dp.id,
                   t1.id_parent_positions AS parent_id,
                   t1.value AS parent_code,
                   dp.id_dictionary
                           FROM dictionary_positions dp
                           left join
                           ( select dr.id_positions, dr.id_parent_positions,dd1.value
                           from  dictionary_relations dr 
                           JOIN dictionary_data dd1 ON dd1.id_position = dr.id_positions
                           JOIN dictionary_attribute da1 ON dd1.id_attribute = da1.id AND da1.alt_name = 'PARENT_CODE'
                           where :dt between dr.start_date and dr.finish_date and  :dt  between dd1.start_date and dd1.finish_date
                           ) t1 on (dp.id = t1.id_positions)
                           WHERE dp.id_dictionary = :id_dictionary
                           and exists (select null from dictionary_data dd 
                            where dd.value  like '%'||:code||'%' and dd.id_position =dp.id  and :dt between dd.start_date and dd.finish_date)
                  ),
                       attributes AS (
                  select  pd.id,
                               pd.parent_id,
                               pd.parent_code,
                               da.name AS attr_name,
                               dd.value AS attr_value from position_data pd
                  join dictionary_attribute da  on pd.id_dictionary =da.id_dictionary
                  left outer join dictionary_data dd on (dd.id_position =pd.id and dd.id_attribute =da.id )
                   where  :dt between dd.start_date and dd.finish_date
                        )
                       SELECT
                           id,
                           parent_id,
                           parent_code,
                           json_agg(
                               json_build_object('name', attr_name, 'value', attr_value)
                           ) AS attrs
                       FROM attributes
                       GROUP BY id, parent_id, parent_code
                       ORDER BY id;
                       """
        rows = await database.fetch_all(sql, {'id_dictionary': dictionary_id, 'code': str, 'dt': date})
        return [schemas.DictionaryPosition(**dict(row)) for row in rows]


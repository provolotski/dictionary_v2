import logging

from datetime import date
from typing import List, Optional, Dict, Set


import pandas as pd
import asyncio
from pandas import NA

from database import database
from config import LOG_FILE, LOG_LEVEL


logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logging.getLogger("databases").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


class AttributeManager:
    NULL_VALUES = ('nan', 'none','null','')
    BATCH_SIZE = 1000

    @staticmethod
    async def _fetch_dates(dictionary_id: int) -> Dict[str, date]:
        """Получаем сразу все даты для справочника одним запросом"""
        sql = 'SELECT start_date, finish_date FROM dictionary WHERE id = :id'
        row = await database.fetch_one(sql, {'id': dictionary_id})
        return {'start_date': row['start_date'], 'finish_date': row['finish_date']}

    @staticmethod
    async def _batch_create_positions(dictionary_id: int, count: int) -> List[int]:
        """Создаем несколько позиций одним запросом"""
        sql = """
            INSERT INTO dictionary_positions (id_dictionary)
            SELECT :id_dictionary FROM generate_series(1, :count)
            RETURNING id
        """
        rows = await database.fetch_all(sql, {'id_dictionary': dictionary_id, 'count': count})
        return [row['id'] for row in rows]

    @staticmethod
    async def _get_attributes_info(dictionary_id: int) -> Dict[str, Dict]:
        """Получаем всю информацию об атрибутах одним запросом"""
        sql = """
                SELECT id, alt_name FROM dictionary_attribute
                WHERE id_dictionary = :id_dictionary AND alt_name IS NOT NULL
            """
        rows = await database.fetch_all(sql, {'id_dictionary': dictionary_id})
        return {row['alt_name']: {'id': row['id']} for row in rows}

    @staticmethod
    async def _batch_insert_data(data: List[Dict]) -> None:
        """Пакетная вставка данных"""
        sql = """
               INSERT INTO dictionary_data 
               (id_position, id_attribute, start_date, finish_date, value)
               VALUES (:id_position, :id_attribute, :start_date, :finish_date, :value)
           """
        await database.execute_many(sql, data)





    @staticmethod
    def _dates_overlap(row: dict, parent_row: dict) -> bool:
        return parent_row['start_date'] < row['finish_date'] and parent_row['finish_date'] > row['start_date']

    @staticmethod
    async def _update_position_relations(position_id: int, dictionary_id: int) -> None:
        await database.execute(
            'DELETE FROM dictionary_relations WHERE id_positions = :id',
            {'id': position_id}
        )

        parent_codes = await database.fetch_all(
            'SELECT dd.value, dd.start_date, dd.finish_date '
            'FROM dictionary_data dd '
            'JOIN dictionary_attribute da ON dd.id_attribute = da.id '
            'WHERE dd.id_position = :id_position AND da.alt_name = \'PARENT_CODE\'',
            {'id_position': position_id}
        )

        for code in parent_codes:
            candidates = await database.fetch_all(
                'SELECT dd.id_position, dd.start_date, dd.finish_date '
                'FROM dictionary_data dd '
                'JOIN dictionary_attribute da ON dd.id_attribute = da.id '
                'WHERE da.id_dictionary = :id_dictionary '
                'AND da.alt_name = \'CODE\' '
                'AND dd.value = CAST(:parent_code AS text) '
                'ORDER BY start_date',
                {'id_dictionary': dictionary_id, 'parent_code': code['value']}
            )

            for candidate in candidates:
                if not AttributeManager._dates_overlap(code, candidate):
                    continue

                try:
                    await database.execute(
                        'INSERT INTO dictionary_relations '
                        '(id_positions, id_parent_positions, start_date, finish_date) '
                        'VALUES(:id_positions, :id_parent_positions, :start_date, :finish_date)',
                        {
                            'id_positions': position_id,
                            'id_parent_positions': candidate['id_position'],
                            'start_date': max(code['start_date'], candidate['start_date']),
                            'finish_date': min(code['finish_date'], candidate['finish_date'])
                        }
                    )

                except Exception as e:
                    logger.error(
                        f'Failed to insert relation: position={position_id}, '
                        f'parent={candidate["id_position"]}. Error: {str(e)}'
                    )
        logger.info(
            f'Successfully inserted relation: '
            f'position={position_id} '
        )



    @staticmethod
    async def import_data(dictionary_id: int, df: pd.DataFrame)->None:
        """
        импорт значений справочника из pandas dataframe
        :param dictionary_id: идентификатор справочника
        :param df: импортированный dataframe
        :return:
        """

        if not {'CODE', 'NAME'}.issubset(df.columns):
            logger.error('Required columns CODE or NAME are missing')
            raise ValueError('DataFrame must contain CODE and NAME columns')

        # Получаем все необходимые данные одним запросом
        dates = await AttributeManager._fetch_dates(dictionary_id)
        attributes_info = await AttributeManager._get_attributes_info(dictionary_id)

        # Фильтруем DataFrame один раз
        valid_rows = df[df['CODE'].notna() & df['NAME'].notna()]
        total_rows = len(valid_rows)

        # Создаем все позиции одним запросом
        position_ids = await AttributeManager._batch_create_positions(dictionary_id, total_rows)

        # Подготавливаем данные для пакетной вставки
        data_to_insert = []
        valid_attributes = set(attributes_info.keys()) & set(df.columns)

        for idx, (_, row) in enumerate(valid_rows.iterrows()):
            position_id = position_ids[idx]
            for attr in valid_attributes:
                value = str(row[attr])
                clean_value = None if value.strip().lower() in AttributeManager.NULL_VALUES else value

                data_to_insert.append({
                    'id_position': position_id,
                    'id_attribute': attributes_info[attr]['id'],
                    'start_date': dates['start_date'],
                    'finish_date': dates['finish_date'],
                    'value': clean_value
                })

                # Вставляем батчами
                if len(data_to_insert) >= AttributeManager.BATCH_SIZE:
                    await AttributeManager._batch_insert_data(data_to_insert)
                    data_to_insert = []

        # Вставляем оставшиеся записи
        if data_to_insert:
            await AttributeManager._batch_insert_data(data_to_insert)

        # Генерируем отношения
        await AttributeManager.generate_relations_for_dictionary(dictionary_id)

    @staticmethod
    async def generate_relations_for_dictionary(dictionary_id: int) -> None:

        """
              Расставляем иерархию для справочника
              :param dictionary_id:  идентификатор справочника
              :return:
              """

        """Оптимизированный метод генерации отношений"""
        # Получаем все позиции одним запросом
        positions = await database.fetch_all(
            'SELECT id FROM dictionary_positions WHERE id_dictionary = :id_dictionary',
            {'id_dictionary': dictionary_id}
        )

        # Используем asyncio.gather для параллельного выполнения
        tasks = [
            AttributeManager._update_position_relations(position['id'], dictionary_id)
            for position in positions
        ]
        await asyncio.gather(*tasks)

    @staticmethod
    async def _update_position_relations(position_id: int, dictionary_id: int) -> None:
        """Оптимизированный метод обновления отношений для позиции"""
        try:
            # Удаляем старые отношения
            await database.execute(
                'DELETE FROM dictionary_relations WHERE id_positions = :id',
                {'id': position_id}
            )

            # Получаем все родительские коды одним запросом
            parent_codes = await database.fetch_all(
                '''SELECT dd.value, dd.start_date, dd.finish_date 
                FROM dictionary_data dd 
                JOIN dictionary_attribute da ON dd.id_attribute = da.id 
                WHERE dd.id_position = :id_position AND da.alt_name = 'PARENT_CODE' ''',
                {'id_position': position_id}
            )

            # Подготавливаем данные для пакетной вставки
            relations_to_insert = []

            for code in parent_codes:
                candidates = await database.fetch_all(
                    '''SELECT dd.id_position, dd.start_date, dd.finish_date 
                    FROM dictionary_data dd 
                    JOIN dictionary_attribute da ON dd.id_attribute = da.id 
                    WHERE da.id_dictionary = :id_dictionary 
                    AND da.alt_name = 'CODE' 
                    AND dd.value = CAST(:parent_code AS text)''',
                    {'id_dictionary': dictionary_id, 'parent_code': code['value']}
                )

                for candidate in candidates:
                    if not (code['start_date'] < candidate['finish_date'] and
                            code['finish_date'] > candidate['start_date']):
                        continue

                    relations_to_insert.append({
                        'id_positions': position_id,
                        'id_parent_positions': candidate['id_position'],
                        'start_date': max(code['start_date'], candidate['start_date']),
                        'finish_date': min(code['finish_date'], candidate['finish_date'])
                    })

            # Вставляем все отношения одним запросом
            if relations_to_insert:
                await database.execute_many(
                    '''INSERT INTO dictionary_relations 
                    (id_positions, id_parent_positions, start_date, finish_date)
                    VALUES (:id_positions, :id_parent_positions, :start_date, :finish_date)''',
                    relations_to_insert
                )

            logger.info(f'Successfully updated relations for position: {position_id}')
        except Exception as e:
            logger.error(f'Failed to update relations for position {position_id}: {str(e)}')
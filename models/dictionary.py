import logging

from schemas import DictionaryIn, DictionaryOut
from  database import database
from config import LOG_FILE,LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)


async def get_dictionaries() -> list[DictionaryOut]:
    sql = "select id, name, code from dictionary"
    rows = await database.fetch_all(sql)
    return [DictionaryOut(**dict(row)) for row in rows]


async def create_new_dictionary(dictionary: DictionaryIn):
    logger.debug('creating new models')
    # sql = 'insert into models (name, code, description, start_date, finish_date, change_date, name_eng, name_bel, description_eng, description_bel, gko, organization, classifier, id_status, id_type) values (:name, :code, :description, :start_date, :finish_date, :change_date, :name_eng, :name_bel, :description_eng, :description_bel, :gko, :organization, :classifier, :id_status, :id_type) returning id'
    sql = 'insert into dictionary (name, code, description) values (:name, :code, :description) returning id'
    dic_id = await database.execute(sql, values=dictionary.model_dump())

    return dic_id
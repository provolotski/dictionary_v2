import logging
from fastapi.responses import JSONResponse
from fastapi import APIRouter
from schemas import DictionaryOut, DictionaryIn
import  models.dictionary as eisgs_dict
import crud


from config import LOG_FILE,LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

logger = logging.getLogger(__name__)




dict_router = APIRouter(prefix="/models", tags=["Dictionary"])


@dict_router.get("/", response_model=list[DictionaryOut])
async def read_dictionaries():
    """
    получение всех справочников
    """
    return await crud.get_dictionaries()

@dict_router.post("/newDictionary", response_model=DictionaryIn)
async def create_new_dictionary(dictionary: DictionaryIn):
    """
    Создаем новый справочник
    :param dictionary:
    :return:
    """
    logger.debug('создаем новый справочник')
    await eisgs_dict.create_new_dictionary(dictionary)
    return JSONResponse(content={"message":' errmessage'}, status_code=200)

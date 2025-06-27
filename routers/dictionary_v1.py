import logging
from fastapi import APIRouter, Security
from config import LOG_FILE, LOG_LEVEL
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader


logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(name)-30s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

dict_router = APIRouter(prefix="/dict", tags=["dict"])


@dict_router.get("/getList")
async def dict_list(
    authorization_header: str = Security(
        APIKeyHeader(name="Authorization", auto_error=False)
    ),
):
    """
    Получение перечня всех получаемых справочников
    :param authorization_header: access token аутентификации, передается в заголовке
    :return: json list
    """
    # status_code, errmessage, username = await check_token(authorization_header)
    status_code, errmessage = 200, "ok"
    logger.debug(status_code)
    if status_code == 200:
        # logger.info(f'Пользователь {username} запросил перечень справочников')
        # result = await get_dicionaries_list()
        # return result
        return []
    else:
        return JSONResponse(content={"message": errmessage}, status_code=status_code)

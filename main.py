from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI, APIRouter
from routers.dictionary import dict_router
from routers.dictionary_v1 import dict_router as dict_router1
from database import database
from config import settings
from fastapi.middleware.cors import CORSMiddleware

from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)

from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=settings.log_level,
    format=settings.log_format,
    datefmt=settings.log_date,
    handlers=[logging.FileHandler(settings.log_file), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Trying connect to database")
    await database.connect()
    logger.info("Connected to database")
    yield
    await database.disconnect()
    logger.info("Disconnected from database")


api_router = APIRouter(prefix="/api/v2")
api_router.include_router(dict_router)

api_router_v1 = APIRouter(prefix="/api/v1")
api_router_v1.include_router(dict_router1)

app = FastAPI(
    lifespan=lifespan,
    title="Сервис доступа к справочникам ЕИСГС",
    summary="Справочники ЕИСГС",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    swagger_ui_parameters={"syntaxHighlight.theme": "obsidian"},
    swagger_js_url="/static/swagger/swagger-ui-bundle.js",
    swagger_css_url="/static/swagger/swagger-ui.css",
)


# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешает все домены (небезопасно для прода!)
    allow_credentials=True,  # Разрешает куки и заголовки авторизации
    allow_methods=["*"],  # Разрешает все методы (GET, POST, PUT и т. д.)
    allow_headers=["*"],  # Разрешает все заголовки
)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="/static/redoc.standalone.js",
    )


app.include_router(api_router)
app.include_router(api_router_v1)

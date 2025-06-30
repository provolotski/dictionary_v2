import logging
import databases
from config import settings
logging.getLogger("database").setLevel(logging.CRITICAL)

DATABASE_URL = (f"postgresql://{settings.postgres_user}:"
                f"{settings.postgres_password}@{settings.postgres_host}:"
                f"{settings.postgres_port}/{settings.postgres_schema}")

database = databases.Database(DATABASE_URL)

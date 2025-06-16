import logging
import databases
logging.getLogger("database").setLevel(logging.CRITICAL)

DATABASE_URL = "postgresql://nsi_user:nsi_user@172.16.251.170:5432/nsi"

database = databases.Database(DATABASE_URL)

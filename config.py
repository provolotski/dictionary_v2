"""
Просто конф
"""
from environs import Env

env = Env()
env.read_env()

# Logger settings
LOG_LEVEL = env.str("LOG_LEVEL")
LOG_FILE = env.str("LOG_FILE")

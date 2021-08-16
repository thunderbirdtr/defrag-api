# Defrag - centralized API for the openSUSE Infrastructure
# Copyright (C) 2021 openSUSE contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

import logging
from fastapi import FastAPI
from pathlib import Path

import os

LOGGER = logging.getLogger(__name__)
# TODO: Change the logformat so that it fits uvicorn
LOGFORMAT = "[%(asctime)s | %(levelname)s] %(message)s"
DEBUG = True
if DEBUG:
    logging.basicConfig(
        format=LOGFORMAT,
        level=logging.DEBUG)
else:
    logging.basicConfig(
        format=LOGFORMAT,
        level=logging.INFO)

LOAD = []

user_env = Path(".env")
has_env = user_env.is_file() or bool(os.environ.get('ENV', False))

if has_env:
    REDIS_HOST = os.environ.get("REDIS_HOST", None)
    REDIS_PORT = int(os.environ.get("REDIS_PORT", None))
    REDIS_PWD = os.environ.get("REDIS_PWD", None)
    BUGZILLA_USER = os.environ.get("BUGZILLA_USER", None)
    BUGZILLA_PASSWORD = os.environ.get("BUGZILLA_PASSWORD", None)
    NO_LOAD = os.environ.get("NO_LOAD", "").split()
    TWITTER_CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY", None)
    TWITTER_CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET", None)
    TWITTER_ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN", None)
    TWITTER_ACCESS_TOKEN_SECRET = os.environ.get(
        "TWITTER_ACCESS_TOKEN_SECRET", None)
else:
    from defrag.config import Config
    REDIS_HOST = Config.REDIS_HOST
    REDIS_PORT = Config.REDIS_PORT
    REDIS_PWD = Config.REDIS_PWD
    BUGZILLA_USER = Config.BUGZILLA_USER
    BUGZILLA_PASSWORD = Config.BUGZILLA_PASSWORD
    NO_LOAD = Config.NO_LOAD
    TWITTER_CONSUMER_KEY = Config.TWITTER_CONSUMER_KEY
    TWITTER_CONSUMER_SECRET = Config.TWITTER_CONSUMER_SECRET
    TWITTER_ACCESS_TOKEN = Config.TWITTER_ACCESS_TOKEN
    TWITTER_ACCESS_TOKEN_SECRET = Config.TWITTER_ACCESS_TOKEN_SECRET

# Initialize app
app = FastAPI(docs_url=None, redoc_url=None)

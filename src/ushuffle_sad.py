#!/usr/bin/env python
"""
兼容Python 2.x 和 3.x 版本
使用SQLAlchemy ORM 搭配后端数据库MySQL 或 SQLite
非原创代码
"""

from distutils.log import warn as printf
from os.path import dirname
from random import randrange as rand
from sqlalchemy import Column, Integer, String, create_engine, exc, platform

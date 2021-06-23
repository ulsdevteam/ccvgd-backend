import hashlib
import os

class Config:
  DEBUG = False
  JSON_AS_ASCII = False
  SQLALCHEMY_DATABASE_URL = "mysql://root:123456@127.0.0.1:3307/ccvg"
  SQLALCHEMY_TRACK_MODIFICATIONS = True
  MAX_CONTENT_LENGTH = 3*1024*1924
  JSON_AS_ASCII= False
  basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

class DevlopConfig(Config):
  DEBUG = True

class PrductConfig(Config):
  pass

import hashlib
import os

# Please change default here!
mysql_host = "localhost"
mysql_username = "root"
mysql_password = "123456"
mysql_port = 3306
mysql_database = "CCVG"

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

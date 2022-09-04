import hashlib
import os


mysql_host = os.getenv("MYSQL_HOST")
mysql_password = os.getenv("MYSQL_ROOT_PASSWORD")
mysql_username = os.getenv("MYSQL_USER")
mysql_port = int(os.getenv("MYSQL_PORT"))
mysql_database = os.getenv("MYSQL_DATABASE")



class Config:
  DEBUG = False
  JSON_AS_ASCII = False
  SQLALCHEMY_DATABASE_URL = "mysql+pymsql://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/sys"
  SQLALCHEMY_TRACK_MODIFICATIONS = True
  MAX_CONTENT_LENGTH = 3*1024*1924
  PORT=5050
  basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

class DevlopConfig(Config):
  DEBUG = True

class PrductConfig(Config):
  pass

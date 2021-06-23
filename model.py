
# import mysql.connector
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from flask import current_app
from manager import create_app
from config import DevlopConfig


app = create_app(DevlopConfig)

db = SQLAlchemy(app)

# class BaseModel(object):
#   def add_update(self):
#     db.session.add(self)
#     db.session.commit()
#
#   def delete(self):
#     db.session.delete(self)
#     db.session.commit()
#
# class GazetteerInformation(db.Model):
#   __tablename__ = 'gazetteerinformation_村志信息'




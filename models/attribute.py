from sqlalchemy import Table, Column
from sqlalchemy.sql.sqltypes import Integer, String
from config.db import meta, engine

attributes = Table("attributes", meta, 
    Column("id", Integer, primary_key=True), 
    Column("code", String(255), nullable=True), 
    Column("label", String(255), nullable=True))

meta.create_all(engine)
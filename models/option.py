from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String
from config.db import meta, engine

options = Table("options", meta, 
    Column("id", Integer, primary_key=True), 
    Column("label", String(255), nullable=True), 
    Column("attribute_id", Integer, ForeignKey("attributes.id")))

meta.create_all(engine)
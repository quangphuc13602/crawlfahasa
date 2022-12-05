from sqlalchemy import Table, Column
from sqlalchemy.sql.sqltypes import Integer, String, Text
from config.db import meta, engine

categories = Table("categories", meta, 
    Column("id", Integer, primary_key=True), 
    Column("name", String(255), nullable=True), 
    Column("path", String(255), nullable=True), 
    Column("title", String(255), nullable=True), 
    Column("description", Text, nullable=True), 
    Column("keywords", String(255), nullable=True), 
    Column("parent_category", Integer))

meta.create_all(engine)

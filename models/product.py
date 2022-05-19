from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.sql.sqltypes import Integer, String, Text, DECIMAL, Float
from config.db import meta, engine

products = Table("products", meta, 
    Column("id", Integer, primary_key=True), 
    Column("product_name", Text, nullable=True), 
    Column("product_finalprice", DECIMAL, nullable=True), 
    Column("product_price", DECIMAL, nullable=True), 
    Column("type_id", String(255), nullable=True),
    Column("type", String(255), nullable=True),
    Column("rating_html", Text, nullable=True), 
    Column("soon_release", String(255), nullable=True), 
    Column("product_url", Text, nullable=True),
    Column("image_src", Text, nullable=True),
    Column("discount", Integer, nullable=True),
    Column("discount_label_html", Text, nullable=True),
    Column("episode", Integer, nullable=True),
    Column("item_code", String(255), nullable=True),
    Column("author", String(255), nullable=True),
    Column("publisher", String(255), nullable=True),
    Column("publish_year", Integer, nullable=True),
    Column("weight", Float, nullable=True),
    Column("size", String(255), nullable=True),
    Column("page_number", Integer, nullable=True),
    Column("material", String(255), nullable=True),
    Column("specification", String(255), nullable=True),
    Column("warning_info", String(255), nullable=True),
    Column("use_guide", Text, nullable=True),
    Column("translator", String(255), nullable=True),
    Column("category_id", Integer, ForeignKey("categories.id")))

meta.create_all(engine)
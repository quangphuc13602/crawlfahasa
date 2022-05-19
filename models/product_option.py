from sqlalchemy import Table, Column, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer
from config.db import meta, engine

products_options = Table("products_options", meta, 
    Column("product_id", Integer, ForeignKey("products.id")),  
    Column("option_id", Integer, ForeignKey("options.id")),
    PrimaryKeyConstraint("product_id", "option_id", name='pro_opt_pk'))

meta.create_all(engine)
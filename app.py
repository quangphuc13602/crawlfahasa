from fastapi import FastAPI
from routes.category import category
from routes.attribute import attribute
from routes.option import option
from routes.product import product
from routes.product_option import product_option

app = FastAPI()

app.include_router(category)

app.include_router(attribute)

app.include_router(option)

app.include_router(product)

app.include_router(product_option)

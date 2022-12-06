from fastapi import FastAPI
from routes.category import category
from routes.product import product

app = FastAPI()

app.include_router(category)
app.include_router(product)
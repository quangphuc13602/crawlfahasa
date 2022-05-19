from fastapi import APIRouter
from fastapi.responses import JSONResponse
from config.db import conn
from models.product_option import products_options
from schemas.schemas import Product_Option

product_option = APIRouter()


def get_product_option(product_id, option_id):
    result = conn.execute(products_options.select().where(
        products_options.c.product_id == product_id and products_options.c.option_id == option_id)).first()
    return result


def get_products_options():
    return conn.execute(products_options.select()).fetchall()


def create_product_option(product_option: Product_Option):
    new_product_option = {"product_id": product_option.product_id,
                          "option_id": product_option.option_id,
                          }
    result = conn.execute(products_options.insert().values(new_product_option))
    return conn.execute(products_options.select().where(products_options.c.option_id == result.c.option_id and products_options.c.product_id == result.c.product_id)).first()


@product_option.get("/products_options", tags=["Product_Option"], response_model=list[Product_Option])
def get_products_options_endpoint():
    result = get_products_options()
    return result


@product_option.get("/products_options/{product_id}?{option_id}", tags=["Product_Option"], response_model=Product_Option)
def get_product_option_endpoint(product_id, option_id):
    result = get_product_option(product_id, option_id)
    return result


@product_option.post("/products_options", tags=["Product_Option"], response_model=list[Product_Option])
def create_products_endpoint(products_options: list[Product_Option]):
    if not products_options:
        return JSONResponse(status_code=404, content={"message": "product option details not found"})
    else:
        products_options_list = []
        for product_option in products_options:
            if (get_product_option(product_option.product_id, product_option.option_id) is None):
                db_product_option = create_product_option(product_option)
                product_option_json = product_option.dict()
                products_options_list.append(product_option_json)
    return JSONResponse(content=products_options_list)




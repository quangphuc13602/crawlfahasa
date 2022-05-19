from select import select
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from config.db import conn
from models.product import products
from models.option import options
from models.attribute import attributes
from models.product_option import products_options
from schemas.schemas import Product
from schemas.schemas import Product_Update


product = APIRouter()


def get_products():
    return conn.execute(products.select()).fetchall()


def get_product(id: int):
    return conn.execute(products.select().where(products.c.id == id)).first()


def create_product(product: Product):
    new_product = {"id": product.id,
                   "product_name": product.product_name,
                   "product_finalprice": product.product_finalprice,
                   "product_price": product.price,
                   "type_id": product.type_id,
                   "type": product.type,
                   "rating_html": product.rating_html,
                   "soon_release": product.soon_release,
                   "product_url": product.product_url,
                   "image_src": product.image_src,
                   "discount": product.discount,
                   "discount_label_html": product.discount_label_html,
                   "episode": product.episode,
                   "item_code": product.item_code,
                   "author": product.author,
                   "publisher": product.publisher,
                   "publish_year": product.publish_year,
                   "weight": product.weight,
                   "size": product.size,
                   "page_number": product.page_number,
                   "material": product.material,
                   "specification": product.specification,
                   "warning_info": product.warning_info,
                   "use_guide": product.use_guide,
                   "translator": product.translator,
                   "category_id": product.category_id}
    result = conn.execute(products.insert().values(new_product))
    return conn.execute(products.select().where(products.c.id == result.lastrowid)).first()


def update_product(id: int, product: Product_Update):
    conn.execute(products.update().values(product_name=product.product_name,
                                          product_finalprice=product.product_finalprice,
                                          product_price=product.price,
                                          type_id=product.type_id,
                                          type=product.type,
                                          rating_html=product.rating_html,
                                          soon_release=product.soon_release,
                                          product_url=product.product_url,
                                          image_src=product.image_src,
                                          discount=product.discount,
                                          discount_label_html=product.discount_label_html,
                                          episode=product.episode,
                                          item_code=product.item_code,
                                          author=product.author,
                                          publisher=product.publisher,
                                          publish_year=product.publish_year,
                                          weight=product.weight,
                                          size=product.size,
                                          page_number=product.page_number,
                                          material=product.material,
                                          specification=product.specification,
                                          warning_info=product.warning_info,
                                          use_guide=product.use_guide,
                                          translator=product.translator,
                                          category_id=product.category_id).where(products.c.id == id))
    return conn.execute(products.select().where(products.c.id == id)).first()


def delete_product(id: int):
    product = get_product(id)
    result = conn.execute(products.delete().where(products.c.id == id))
    return product

# def get_products_by_price(min: int, max: int):
#     return conn.execute(products.select().where(products.c.product_finalprice>=min and products.c.product_finalprice<=max))

def get_products_by_price(min: int, max: int):
    return conn.execute(products.select().where(products.c.product_finalprice >= min and products.c.product_finalprice <= max))


def get_products_by_genres(genres: str):
    return conn.execute(select(products).where(products.c.id == products_options.c.product_id and
                                       options.c.id == products_options.c.option_id and
                                       attributes.c.id == options.c.attribute_id and
                                       attributes.c.code == "genres" and options.c.label.likes('%' + genres + '%')))


@product.get("/products", tags=["Product"], response_model=list[Product])
def get_products_by_price_endpoint(min: int, max: int):
    result = get_products_by_price(min, max)
    return result


@product.get("/products", tags=["Product"], response_model=list[Product])
def get_products_endpoint():
    result = get_products()
    return result


@product.get("/products/{id}", tags=["Product"], response_model=Product)
def get_product_endpoint(id: int):
    result = get_product(id)
    return result


@product.post("/products", tags=["Product"], response_model=list[Product])
def create_products_endpoint(products: list[Product]):
    if not products:
        return JSONResponse(status_code=404, content={"message": "product details not found"})
    else:
        products_list = []
        for product in products:
            if (get_product(product.id) is None):
                db_product = create_product(product)
                product_json = product.dict()
                products_list.append(product_json)
    return JSONResponse(content=products_list)


@product.put("/products/{id}", tags=["Product"], response_model=Product)
def update_product_endpoint(id: int, product: Product_Update):
    result = update_product(id, product)
    return result


@product.delete("/products/{id}", tags=["Product"], status_code=status.HTTP_204_NO_CONTENT)
def delete_product_endpoint(id: int):
    result = delete_product(id)
    return result

@product.get("/products/price/min={min}?max={max}", tags=["Product Endpoint"], response_model=list[Product])
def get_products_by_price_endpoint(min: int, max: int):
    result = get_products_by_price(min, max)
    return result


@product.get("/products/genres/{genres}", tags=["Product Endpoint"], response_model=list[Product])
def get_products_by_genres_endpoint(genres: str):
    result = get_products_by_genres(genres)
    return result




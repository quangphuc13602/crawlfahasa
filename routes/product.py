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
from routes.category import category as cate
import crawl
from models.category import categories
import pandas as pd

product = APIRouter()


def get_products():
    return conn.execute(products.select()).fetchall()


def get_product(id: int):
    return conn.execute(products.select().where(products.c.id == id)).first()


def create_product(product: Product):
    new_product = {"id": product["id"],
                   "product_name": product["product_name"],
                   "product_finalprice": product["product_finalprice"],
                   "product_price": product["product_price"],
                   "type_id": product["type_id"],
                   "type": product["type"],
                   "rating_html": product["rating_html"],
                   "soon_release": product["soon_release"],
                   "product_url": product["product_url"],
                   "image_src": product["image_src"],
                   "discount": product["discount"],
                   "discount_label_html": product["discount_label_html"],
                   "episode": product["episode"],
                   "item_code": product["item_code"] if "item_code" in product else "",
                   "author": product["author"] if "author" in product else "",
                   "publisher": product["publisher"] if "publisher" in product else "",
                   "publish_year": product["publish_year"] if "publish_year" in product else -1,
                   "weight": product["weight"] if "weight" in product else -1,
                   "size": product["size"] if "size" in product else "",
                   "page_number": product["page_number"] if "page_number" in product else -1,
                   "material": product["material"] if "material" in product else "",
                   "specification": product["specification"] if "specification" in product else "",
                   "warning_info": product["warning_info"] if "warning_info" in product else "",
                   "use_guide": product["use_guide"] if "use_guide" in product else "",
                   "translator": product["translator"] if "translator" in product else "",
                   "category_id": product["category_id"]}
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

# @product.get("/products_crawl", tags=["Crawl"])
# def crawl_products():
#     return "hihi"

def get_youngest_child_categories():
    return conn.execute(categories.select().where(categories.c.id.notin_(categories.select().with_only_columns(categories.c.parent_category)))).fetchall()


@product.get("/crawl-products", tags=["Crawl"])
def crawl_products(max_qty: int):
    cate_child_id = [i['id'] for i in get_youngest_child_categories()]

    old_product_id = [i['id'] for i in get_products()]

    new_product_id = []

    new_products = []

    def get_data_product(category_id):
        if max_qty <= len(new_products):
            return "crawl max!"
        try:
            url = crawl.data_url + str(category_id)
            res = crawl.get_data_from_api2(url)

            total = res['total_products']
            url = crawl.url_product_list.format(category_id, total)
            df = pd.DataFrame(res['product_list'])
            df.fillna(value=0, inplace=True)
            df['category_id'] = category_id
            df['product_price'] =  [int(str(p).replace('.', '')) for p in df['product_price']]
            df['product_finalprice'] =  [int(str(p).replace('.', '')) for p in df['product_finalprice']]
            df.rename(columns = {'product_id':'id'}, inplace=True)
            df['id'] = pd.to_numeric(df['id'])
            new_data = [row for row in df.to_dict('records') if row['id'] not in old_product_id and row['id'] not in new_product_id]

            qty = max_qty - len(new_products)
            new_products.extend(new_data[:qty])

            cur_ids = df['id'].values.tolist()
            new_ids = [i for i in cur_ids if i not in new_product_id]
            new_product_id.extend(new_ids)
            return "cate_id: {} | product qty of this id: {} | crawl count: {}".format(category_id,total,len(new_products))
        except:
            print("Crawl product error!")
        

    crawl.runner(cate_child_id, get_data_product)
    crawl.runner(new_products, create_product)

    return {
        "currentProducts": len(old_product_id),
        "new": len(new_products),
        "data": new_products
    }

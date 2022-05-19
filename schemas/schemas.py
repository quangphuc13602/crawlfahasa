from decimal import Decimal
from pydantic import BaseModel
from typing import Optional


class Category(BaseModel):
    id: Optional[int]
    name: str
    path: str
    title: str
    description: str
    keywords: str
    parent_category: int


class Category_Update(BaseModel):
    name: str
    path: str
    title: str
    description: str
    keywords: str
    parent_category: int


class Attribute(BaseModel):
    id: Optional[int]
    code: str
    label: str


class Attribute_Update(BaseModel):
    code: str
    label: str


class Option(BaseModel):
    id: Optional[int]
    label: str
    attribute_id: int


class Option_Update(BaseModel):
    label: str
    attribute_id: int


class Product(BaseModel):
    id: Optional[int]
    product_name: str
    product_finalprice: Decimal
    product_price: Decimal
    type_id: str
    type: str
    rating_html: str
    soon_release: str
    product_url: str
    image_src: str
    discount: int
    discount_label_html: str
    episode: int
    item_code: str
    author: str
    publisher: str
    publish_year: int
    weight: float
    size: str
    page_number: int
    material: str
    specification: str
    warning_info: str
    use_guide: str
    translator: str
    category_id: int


class Product_Update(BaseModel):
    product_name: str
    product_finalprice: Decimal
    product_price: Decimal
    type_id: str
    type: str
    rating_html: str
    soon_release: str
    product_url: str
    image_src: str
    discount: int
    discount_label_html: str
    episode: int
    item_code: str
    author: str
    publisher: str
    publish_year: int
    weight: float
    size: str
    page_number: int
    material: str
    specification: str
    warning_info: str
    use_guide: str
    translator: str
    category_id: int


class Product_Option(BaseModel):
    product_id: int
    option_id: int

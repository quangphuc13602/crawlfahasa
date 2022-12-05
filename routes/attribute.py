from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from config.db import conn
from models.attribute import attributes
from schemas.schemas import Attribute
from schemas.schemas import Attribute_Update

attribute = APIRouter()


def get_attributes():
    return conn.execute(attributes.select()).fetchall()


def get_attribute(id: int):
    return conn.execute(attributes.select().where(attributes.c.id == id)).first()


def create_attribute(attribute: Attribute):
    new_attribute = {"id": attribute.id,
                     "code": attribute.code,
                     "label": attribute.label}
    result = conn.execute(attributes.insert().values(new_attribute))
    return conn.execute(attributes.select().where(attributes.c.id == result.lastrowid)).first()


def update_attribute(id: int, attribute: Attribute_Update):
    conn.execute(attributes.update().values(code=attribute.code,
                                            label=attribute.label).where(attributes.c.id == id))
    return conn.execute(attributes.select().where(attributes.c.id == id)).first()


def delete_attribute(id: int):
    attribute = get_attribute(id)
    result = conn.execute(attributes.delete().where(attributes.c.id == id))
    return attribute


@attribute.get("/attributes", tags=["Attribute"], response_model=list[Attribute])
def get_attributes_endpoint():
    result = get_attributes()
    return result


@attribute.get("/attributes/{id}", tags=["Attribute"], response_model=Attribute)
def get_attribute_endpoint(id: int):
    result = get_attribute(id)
    return result


@attribute.post("/attributes", tags=["Attribute"], response_model=list[Attribute])
def create_attributes_endpoint(attributes: list[Attribute]):
    if not attributes:
        return JSONResponse(status_code=404, content={"message": "attribute details not found"})
    else:
        attributes_list = []
        for attribute in attributes:
            if (get_attribute(attribute.id) is None):
                db_attribute = create_attribute(attribute)
                attribute_json = attribute.dict()
                attributes_list.append(attribute_json)
    return JSONResponse(content=attributes_list)


@attribute.put("/attributes/{id}", tags=["Attribute"], response_model=Attribute)
def update_attribute_endpoint(id: int, attribute: Attribute_Update):
    result = update_attribute(id, attribute)
    return result


@attribute.delete("/attributes/{id}", tags=["Attribute"], status_code=status.HTTP_204_NO_CONTENT)
def delete_attribute_endpoint(id: int):
    result = delete_attribute(id)
    return result


# @attribute.get

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from config.db import conn
from models.option import options
from schemas.schemas import Option
from schemas.schemas import Option_Update

option = APIRouter()


def get_options():
    return conn.execute(options.select()).fetchall()


def get_option(id: int):
    return conn.execute(options.select().where(options.c.id == id)).first()


def create_option(option: Option):
    new_option = {"id": option.id,
                  "label": option.label,
                  "attribute_id": option.attribute_id}
    result = conn.execute(options.insert().values(new_option))
    return conn.execute(options.select().where(options.c.id == result.lastrowid)).first()


def update_option(id: int, option: Option_Update):
    conn.execute(options.update().values(label=option.label,
                                         attribute_id=option.attribute_id).where(options.c.id == id))
    return conn.execute(options.select().where(options.c.id == id)).first()


def delete_option(id: int):
    option = get_option(id)
    result = conn.execute(options.delete().where(options.c.id == id))
    return option


@option.get("/options", tags=["Option"], response_model=list[Option])
def get_options_endpoint():
    result = get_options()
    return result


@option.get("/options/{id}", tags=["Option"], response_model=Option)
def get_option_endpoint(id: int):
    result = get_option(id)
    return result


@option.post("/options", tags=["Option"], response_model=list[Option])
def create_options_endpoint(options: list[Option]):
    if not options:
        return JSONResponse(status_code=404, content={"message": "option details not found"})
    else:
        options_list = []
        for option in options:
            if (get_option(option.id) is None):
                db_option = create_option(option)
                option_json = option.dict()
                options_list.append(option_json)
    return JSONResponse(content=options_list)


@option.put("/options/{id}", tags=["Option"], response_model=Option)
def update_option_endpoint(id: int, option: Option_Update):
    result = update_option(id, option)
    return result


@option.delete("/options/{id}", tags=["Option"], status_code=status.HTTP_204_NO_CONTENT)
def delete_option_endpoint(id: int):
    result = delete_option(id)
    return result





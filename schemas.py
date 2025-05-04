from pydantic import BaseModel
from datetime import date


class DictionaryIn(BaseModel):
    name: str
    code: str
    description: str


class DictionaryOut(DictionaryIn):
    id: int


class Attribute(BaseModel):
    id: int
    name: str
    type: str
    start_date: date
    end_date: date
    required: bool
    capacity: int
    alt_name: str


class DictionaryID(BaseModel):
    id: int


class AttrShown(BaseModel):
    name: str
    value: str


class ListAttr(BaseModel):
    attrs: AttrShown


class DictionaryPosition(BaseModel):
    id: int
    parent_id: int
    parent_code: str
    attr: ListAttr

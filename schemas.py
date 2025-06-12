from pydantic import BaseModel
from datetime import date
from typing import Optional


class DictionaryIn(BaseModel):
    name: str
    code: str
    description: str
    start_date: date
    finish_date: Optional[date] = date(9999, 12, 31)
    name_eng: Optional[str]
    name_bel: Optional[str]
    description_eng: Optional[str]
    description_bel: Optional[str]
    gko: str
    organization: str
    classifier: Optional[str]
    id_status: int = 0
    id_type: int


class DictionaryOut(DictionaryIn):
    id: int


class AttributeIn(BaseModel):
    name: str
    id_attribute_type: int
    start_date: date
    finish_date: date
    required: bool
    capacity: int
    alt_name: str


class AttributeOut(AttributeIn):
    id: int


class AttributeDict(AttributeIn):
    id_dictionary: int


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

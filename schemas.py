from pydantic import BaseModel,Field,validator
from datetime import date, datetime
from typing import Optional,List
import json


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
    value: Optional[str] =None


class ListAttr(BaseModel):
    attrs: AttrShown


class DictionaryPosition(BaseModel):
    id: int
    parent_id: Optional[int] = Field(None, alias="parent_id")  # Разрешаем NULL
    parent_code: Optional[str] = Field(None, alias="parent_code")  # Разрешаем NULL
    attrs: List[AttrShown] = Field(..., alias="attrs")  # Ожидаем список

    @validator('attrs', pre=True)
    def parse_attrs(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return []
        return v or []  # Если None - возвращаем пустой список

    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            datetime.date: lambda v: v.isoformat()
        }



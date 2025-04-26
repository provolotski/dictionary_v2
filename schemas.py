from pydantic import BaseModel
from datetime import date

class DictionaryIn(BaseModel):
    name: str
    code: str
    description: str
    # start_date: date
    # end_date: date
    # name_eng:str
    # name_bel:str



class DictionaryOut(DictionaryIn):
    id:int


class Attribute(BaseModel):
    id:int
    name:str
    type:str
    start_date:date
    end_date:date
    required:bool
    capacity: int


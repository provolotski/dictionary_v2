from pydantic import BaseModel, Field, validator, field_validator
from datetime import date  # , datetime
from typing import Optional, List
import json


class DictionaryIn(BaseModel):
    """
    Базовая модель описания справочника
    - **name**: Наименование на русском языке
    - **code**: Уникальный код справочника
    - **description**: Описание справочника
    - **start_date**: Дата начала действия
    - **finish_date**: Дата окончания действия (по умолчанию 31.12.9999)
    - **name_eng**: Наименование на английском (опционально)
    - **name_bel**: Наименование на белорусском (опционально)
    - **description_eng**: Описание на английском (опционально)
    - **description_bel**: Описание на белорусском (опционально)
    - **gko**: Код ГКО
    - **organization**: Организация-владелец справочника
    - **classifier**: Код классификатора (опционально)
    - **id_status**: Статус записи (по умолчанию 0)
    - **id_type**: Тип справочника

    """

    name: str = Field(..., description="Наименование на русском языке")
    code: str = Field(..., description="Уникальный код справочника")
    description: str = Field(..., description="Описание справочника")
    start_date: date = Field(..., description="Дата начала действия")
    finish_date: Optional[date] = Field(
        date(9999, 12, 31),
        description="Дата окончания действия (по умолчанию 31.12.9999)",
    )
    name_eng: Optional[str] = Field(
        ..., description="Наименование справочника на английском языке (опционально)"
    )
    name_bel: Optional[str] = Field(
        ..., description="Наименование справочника на белорусском языке (опционально)"
    )
    description_eng: Optional[str] = Field(
        ..., description="Описание справочника на английском языке (опционально)"
    )
    description_bel: Optional[str] = Field(
        ..., description="Описание справочника на белорусском языке (опционально)"
    )
    gko: str = Field(
        ..., description="Государственный орган, ответственный за ведение справочника"
    )
    organization: str = Field(
        ..., description="Организация, ответственная  за ведение справочника"
    )
    classifier: Optional[str] = Field(
        ...,
        description="Классификатор на базе которого строится справочник(опционально)",
    )
    id_status: int = Field(
        0,
        description="текущий статус справочника: "
        "0(по умолчанию)-действующий, "
        "1- недействующий",
    )
    id_type: int = Field(..., description="Тип справочника")


class DictionaryOut(DictionaryIn):
    """
    Базовая модель описания справочника, включая идентификатор
    - **id**: Идентификатор справочника
    """

    id: int = Field(..., description="Уникальный ID справочника в базе данных")


class AttributeIn(BaseModel):
    """
    Базовая модель описания атрибута справочника
    """

    name: str = Field(..., description="Наименование атрибута")
    id_attribute_type: int = Field(..., description="Тип атрибута")
    start_date: date = Field(..., description="Начало действия атрибута")
    finish_date: date = Field(..., description="Окончание действия атрибута")
    required: bool = Field(..., description="Обязательность атрибута")
    capacity: int = Field(..., description="Размерность атрибута")
    alt_name: str = Field(..., description="альтернативное имя атрибута для загрузки")

    @field_validator("alt_name")
    @classmethod
    def validate_alt_name(cls, v):
        return v.upper() if v else None


class AttributeOut(AttributeIn):
    """
    Расширенная модель атрибута
    """

    id: int = Field(..., description="Уникальный идентификатор атрибута")


class AttributeDict(AttributeIn):
    """
    Модель атрибута с привязкой к справочнику
    """

    id_dictionary: int = Field(..., description="Идентификатор справочника")


class DictionaryID(BaseModel):
    """Сокращенная модель справочника"""

    id: int = Field(..., description="Идентификатор справочника")


class AttrShown(BaseModel):
    """
    Описание значений атрибутов key-value
    """

    name: str = Field(..., description="наименование атрибута ALTNAME")
    value: Optional[str] = Field(
        None, description="Значение атрибута (может быть пустым)"
    )


class DictionaryPosition(BaseModel):
    id: int
    parent_id: Optional[int] = Field(
        None, alias="parent_id", description="Идентификатор родительской позиции"
    )  # Разрешаем NULL
    parent_code: Optional[str] = Field(
        None, alias="parent_code", description="Код родительской позиции"
    )  # Разрешаем NULL
    attrs: List[AttrShown] = Field(
        ..., alias="attrs", description="список атрибутов"
    )  # Ожидаем список

    @validator("attrs", pre=True)
    @classmethod
    def parse_attrs(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return []
        return v or []  # Если None - возвращаем пустой список

    # class Config:
    #     allow_population_by_field_name = True
    #     json_encoders = {datetime.date: lambda v: v.isoformat()}

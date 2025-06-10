import pytest
from httpx import AsyncClient,ASGITransport
from fastapi import FastAPI
from unittest.mock import AsyncMock, patch, ANY
from routers.dictionary import dict_router


app = FastAPI()
app.include_router(dict_router)
transport = ASGITransport(app=app)

@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.create_new_dictionary", new_callable=AsyncMock)
async def test_create_new_dictionary(mock_create):
    payload = { "name": "Test Dictionary", "code": "test_001","description":"test data mock"}
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/newDictionary", json=payload)

    assert response.status_code == 200
    assert response.json() == {"message": " errmessage"}
    mock_create.assert_awaited_once()


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionaries", return_value=[])
async def test_list_dictionaries(mock_list):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/list")

    assert response.status_code == 200
    assert isinstance(response.json(), list)
    mock_list.assert_awaited_once()


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_structure", return_value=[])
async def test_get_dictionary_structure(mock_structure):

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/structure/?dictionary=1")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    mock_structure.assert_awaited_once_with(1)


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_values", return_value=[])
async def test_get_dictionary_values(mock_get):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/dictionary/?dictionary=1")

    assert response.status_code == 200
    assert isinstance(response.json(), list)
    mock_get.assert_awaited()


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_position_by_code", return_value={"code": "A1"})
async def test_get_value_by_code(mock_get):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/dictionaryValueByCode/?dictionary=1&code=A1")

    assert response.status_code == 200
    assert response.json() == {"code": "A1"}
    mock_get.assert_awaited()


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_position_by_id", return_value={"id": 1})
async def test_get_value_by_id(mock_get):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/dictionaryValueByID?dictionary=1&position_id=10")

    assert response.status_code == 200
    assert response.json() == {"id": 1}
    mock_get.assert_awaited()


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.find_dictionary_by_name", return_value={"id": 1, "name": "Test"})
async def test_find_dictionary_by_name(mock_find):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/findDictionaryByName?name=Test")

    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Test"}
    mock_find.assert_awaited_once_with("Test")


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.find_dictionary_by_name", return_value={"id": 2, "name": "Other"})
async def test_find_dictionary_value(mock_find_value):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/findDictionaryValue?dictionary=1&findstr=Search")

    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "Other"}
    mock_find_value.assert_awaited_once_with("Search")


# продолжение существующего файла test_dictionary_router.py

@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_structure", return_value=[])
async def test_post_dictionary_structure(mock_structure):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/structure/", params={"dictionary": 1})

    assert response.status_code == 200
    assert isinstance(response.json(), list)
    mock_structure.assert_awaited_once_with(1)


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_values", return_value=[])
async def test_post_get_dictionary_values(mock_get):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/dictionary/", params={"dictionary": 1})

    assert response.status_code == 200
    assert isinstance(response.json(), list)
    mock_get.assert_awaited()


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_position_by_code", return_value={"code": "A1"})
async def test_post_value_by_code(mock_get):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/dictionaryValueByCode/", params={"dictionary": 1, "code": "A1"})

    assert response.status_code == 200
    assert response.json() == {"code": "A1"}
    mock_get.assert_awaited()


@pytest.mark.asyncio
async def test_post_value_by_code_missing_param():
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/dictionaryValueByCode/", params={"dictionary": 1})

    assert response.status_code == 422  # FastAPI validation error


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.get_dictionary_position_by_id", return_value={"id": 10})
async def test_post_value_by_id(mock_get):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/dictionaryValueByID", params={"dictionary": 1, "position_id": 10})

    assert response.status_code == 200
    assert response.json() == {"id": 10}
    mock_get.assert_awaited_once_with(1, 10, ANY)  # pytest.ANY matches any date


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.find_dictionary_by_name", return_value={"id": 1, "name": "MockDict"})
async def test_post_find_dictionary_by_name(mock_find):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/findDictionaryByName", params={"name": "MockDict"})

    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "MockDict"}
    mock_find.assert_awaited_once_with("MockDict")


@pytest.mark.asyncio
@patch("routers.dictionary.eisgs_dict.find_dictionary_by_name", return_value={"id": 2, "name": "FoundValue"})
async def test_post_find_dictionary_value(mock_find):
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/models/findDictionaryValue", params={"dictionary": 1, "findstr": "value"})

    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "FoundValue"}
    mock_find.assert_awaited_once_with("value")


@pytest.mark.asyncio
async def test_get_dictionary_value_by_code_missing_code():
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/dictionaryValueByCode/?dictionary=1")

    assert response.status_code == 422  # Отсутствует обязательный параметр "code"


@pytest.mark.asyncio
async def test_get_dictionary_value_by_id_missing_position_id():
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/models/dictionaryValueByID?dictionary=1")

    assert response.status_code == 422  # Отсутствует обязательный параметр "position_id"
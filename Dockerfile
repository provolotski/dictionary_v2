FROM python:3.12
LABEL authors="ViachaslauProvolocki"

ENV LOG_FILE=logs/dictionaryAPI.log
ENV LOG_LEVEL=DEBUG


# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Копируем файл зависимостей в контейнер
COPY requirements.txt requirements.txt

# Устанавливаем зависимости
RUN pip install --no-cache-dir --upgrade -r requirements.txt


# Копируем содержимое текущего каталога в контейнер
COPY . .

# Указываем команду для запуска приложения
CMD ["fastapi", "run", "main.py", "--host", "0.0.0.0", "--port", "9092"]
# Используем базовый образ с Python
FROM python:3.11.9-alpine

# Устанавливаем рабочий каталог
WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY . .

# Открываем порт, на котором будет работать FastAPI
EXPOSE 7777

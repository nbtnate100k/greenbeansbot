FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt bot.py ./
COPY assets ./assets
RUN mkdir -p data \
    && pip install --no-cache-dir -r requirements.txt

CMD ["python", "bot.py"]

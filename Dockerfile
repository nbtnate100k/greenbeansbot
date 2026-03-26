FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt bot.py ./
COPY assets ./assets
RUN pip install --no-cache-dir -r requirements.txt \
    && mkdir -p data

CMD ["python", "bot.py"]

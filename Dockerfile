FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt bot.py ./
RUN mkdir -p assets data \
    && pip install --no-cache-dir -r requirements.txt

# Banner for /start: Railway → Settings → Variables: BOT_HEADER_IMAGE=/app/assets/header.png
# and add a volume at /app/assets, OR paste a build block below if assets/ is in your git repo:
# COPY assets ./assets

CMD ["python", "bot.py"]

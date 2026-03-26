FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt bot.py ./
RUN pip install --no-cache-dir -r requirements.txt \
    && mkdir -p data

# Optional: add COPY assets ./assets if you use header.png for /start
CMD ["python", "bot.py"]

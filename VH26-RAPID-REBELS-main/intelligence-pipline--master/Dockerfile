FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp>=3.9.0 kafka-python>=2.0.2

COPY *.py ./
COPY dashboard/ ./dashboard/

ENV PYTHONUNBUFFERED=1

EXPOSE 8081

CMD ["python", "run_both.py"]
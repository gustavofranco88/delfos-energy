FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir sqlalchemy psycopg2-binary

CMD ["python", "-m", "etl.database"]
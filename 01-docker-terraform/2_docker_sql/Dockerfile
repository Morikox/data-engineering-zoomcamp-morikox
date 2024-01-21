FROM python:3.9.9

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY custom_ingest.py custom_ingest.py 

ENTRYPOINT [ "python", "custom_ingest.py" ]
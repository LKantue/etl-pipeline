FROM python:3.8.3-slim

WORKDIR /app

COPY . /app

RUN pip3 install psycopg2-binary pandas boto3

CMD ["python3", "./main.py"]


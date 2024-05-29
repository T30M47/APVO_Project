FROM python:3.8

WORKDIR /app

ADD https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh /usr/local/bin/wait-for-it
RUN chmod +x /usr/local/bin/wait-for-it

COPY requirements.txt .
COPY postgresql-42.7.1.jar .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install psycopg2-binary

COPY . .


#CMD ["python", "create_db.py"]
CMD ["wait-for-it", "postgres_transactions:5432", "--timeout=10", "--", "python", "create_db.py"]
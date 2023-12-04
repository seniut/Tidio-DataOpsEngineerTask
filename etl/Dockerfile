FROM python:3.8

COPY . .

RUN pip install --no-cache-dir -r ./requirements.txt

RUN pytest ./tests/test_etl_flow.py

CMD ["python", "./etl_flow.py"]

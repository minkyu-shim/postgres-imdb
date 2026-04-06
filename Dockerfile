FROM python:3.11

WORKDIR /app

COPY python_part/requirements.txt .

RUN pip install -r requirements.txt

COPY python_part/ .

CMD ["python", "main.py"]
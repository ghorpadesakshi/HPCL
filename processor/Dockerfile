# processor/Dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_app.py /app/spark_app.py
CMD ["python", "spark_app.py"]

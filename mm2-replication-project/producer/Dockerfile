FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY commit_log_producer.py .

ENTRYPOINT ["python", "commit_log_producer.py"]

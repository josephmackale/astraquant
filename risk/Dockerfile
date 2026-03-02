FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --default-timeout=300

COPY . .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8003"]

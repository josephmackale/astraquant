FROM python:3.10-slim

WORKDIR /app

# Copy only API requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy ONLY the API code
COPY . .

# API should expose its own port (e.g. 8000)
EXPOSE 8000

CMD ["uvicorn", "api_gateway:app", "--host", "0.0.0.0", "--port", "8000"]

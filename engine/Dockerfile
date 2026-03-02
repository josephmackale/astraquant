FROM python:3.10-slim

# set working directory inside container
WORKDIR /app

# copy only engine requirements
COPY engine/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# copy the entire project (since context is now ".")
COPY . .

# expose engine folder as Python package + allow spotware_pb top-level imports
ENV PYTHONPATH="/app:/app/engine/feeds/spotware_pb"

# run engine
CMD ["uvicorn", "engine.main:app", "--host", "0.0.0.0", "--port", "8001"]

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy package into a proper Python package directory
COPY . /app/workqueue/

EXPOSE 8200

CMD ["python", "-m", "workqueue", "coordinator", "--host", "0.0.0.0", "--port", "8200", "-c", "/app/workqueue/config.yaml"]

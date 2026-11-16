FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY queue_app queue_app
CMD ["uvicorn","queue_app.api:app","--host","0.0.0.0","--port","8000"]

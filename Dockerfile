# One image for all the Python scripts. The compose file picks which one to run.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py .

# Print output right away so `docker compose logs -f` shows it live.
ENV PYTHONUNBUFFERED=1
CMD ["python", "producer.py"]

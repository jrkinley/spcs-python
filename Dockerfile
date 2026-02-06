FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY imf_datamapper_api.py .

CMD ["python", "imf_datamapper_api.py"]

# Had to modify the original dockerfile
# Issues with building on my computer
FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

CMD ["python3", "main.py"]

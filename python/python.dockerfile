FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "data_generator.py"]

# docker build -t dg . -f python.dockerfile
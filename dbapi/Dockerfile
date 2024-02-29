FROM python:3.9.6-slim
EXPOSE 8080
WORKDIR /flask_test_dockerfile
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip install python-dotenv
COPY . .
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "app:app"]







# FROM python:3.9.6-slim
#
# WORKDIR /app
#
# COPY . /app
#
# RUN pip install --no-cache-dir -r requirements.txt
#
# EXPOSE 8080
#
# CMD ["flask", "run", "--host", "0.0.0.0", "--port", "8080"]

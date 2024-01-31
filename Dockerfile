FROM python:3.8.13-slim-buster
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN pip install -r src/requirements.txt

#When plugin run with rest connector default port is 8080
EXPOSE 8080

CMD ["python", "src/app/main.py"]
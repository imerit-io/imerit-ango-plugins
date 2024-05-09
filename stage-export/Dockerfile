FROM python:3.8.13-slim-buster
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt

#Enable this line for GCP Cloud run
#RUN nohup  python3 -m http.server 8080 &

CMD ["python", "stage_export.py"]
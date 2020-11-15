FROM python:3.5.9-alpine3.11

RUN pip install influxdb 

RUN pip install requests

RUN pip install tzlocal

WORKDIR /code

COPY sampler.py .

CMD ["python", "-u", "./sampler.py"]
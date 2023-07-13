FROM osgeo/gdal:ubuntu-small-latest

RUN apt-get update && DEBIAN_FRONTEND="noninteractive" apt-get install -y python3-pip libpq-dev tzdata

ENV TZ America/Sao_Paulo

#Copy project
COPY . /app

#Set workdir
WORKDIR /app

#Install project dependencies
RUN pip3 install -r requirements.txt

CMD [ "python3","main.py"]
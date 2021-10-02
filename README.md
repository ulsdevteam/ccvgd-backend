# Configuration expectations

## Mysql is database

+ MAX_CONTENT_LENGTH = 3*1024*1924

## Database connection field 

### The username is default root and password is 123456


## As we have Chinese in the database and coding, so set 

+ JSON_AS_ASCII= False

## Opening DEBUG mode
```
class DevlopConfig(Config):
  DEBUG = True
```

# Running command

## python3 app.py runserver -p port_num


# Deploy environment

## The working dir is 
```
- ccvgd-backend
  - app.py 
  - ...
- Dockerfile
``` 
  
So in docker file I create

```
FROM python:3.6


ADD . /ccvgd-backend
WORKDIR /ccvgd-backend
ENV PYTHONPATH=/ccvgd-backend

# Install any needed packages specified in requirements.txt
COPY requirements.txt /ccvgd-backend
RUN pip3 install -r requirements.txt

# Run app.py when the container launches
COPY app.py /app
CMD python3 app.py runserver -p 5050
```

Then run 

`docker build -t ccvgd_backend:latest .`

The result is: run `docker images`

|REPOSITORY  |             TAG    |   IMAGE ID   |    CREATED   |       SIZE|
|  ----  | ----  |  ----  | ----  |  ----  |
|ccvgd-backend       |              latest  |  xxx  | 49 seconds ago |  1.85GB|

**I tried this and it worked on my laptop, so if you want to change the port and path, please go ahead!**


Then exec this docker image to a container

`docker run --name ccvgd -d ccvgd_backend -p 5050:5050` The port should be mapped from inside 5050 to outside 5050, and the name you can change.

If you want to see details inside this docker file, just use `docker exec -it ccvgd /bin/bash` to get into docker container.


## Docker file and deploy our app through and I add the docker file **composed flask and mysql** into Dockerfile.

### This compose function I write in **docker-compose.yml**

```
version: "1"
services:
  mysql:
    image: mysql:5.7
    ports:
      - "3306:3306"
    environment:
      - MYSQL_ROOT_PASSWORD=123456
    volumes:
      - ./db:/docker-entrypoint-initdb.d/:ro
  ccvg:
    build: ./
    ports:
      - "5000:5000"
    depends_on:
      - mysql
    restart: always
```

The build path of this ccvg I set `build: ./` so the absolute path of this docker-compose.yml is "./ccvgd-backend/docker-compose.yml"

## Use command `docker-compose up` to compose.




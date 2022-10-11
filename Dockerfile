FROM python:3.9

RUN apt-get update
RUN apt-get install --yes apache2 apache2-dev
RUN apt-get -y install python3-pip
RUN apt-get install libapache2-mod-wsgi-py3
RUN pip install --upgrade pip

RUN wget https://github.com/GrahamDumpleton/mod_wsgi/archive/refs/tags/4.9.0.tar.gz \
    && tar xvfz 4.9.0.tar.gz \
    && cd mod_wsgi-4.9.0 \
    && ./configure --with-apxs=/usr/bin/apxs --with-python=/usr/local/bin/python3.9 \
    && make \
    && make install \
    && make clean

ADD wsgi.load /etc/apache2/mods-available/wsgi.load

COPY ./apache-flask.conf /etc/apache2/sites-available/apache-flask.conf

RUN a2dissite 000-default.conf
RUN a2ensite apache-flask.conf
RUN a2enmod headers
RUN a2enmod rewrite
RUN a2enmod wsgi

RUN adduser --system --group --disabled-login ccvguser; cd /home/ccvguser/

COPY . /var/www/apache-flask/
RUN chown ccvguser:www-data -R /var/www/apache-flask/
WORKDIR /var/www/apache-flask/
RUN pip install --no-cache-dir -r requirements.txt

RUN ln -sf /proc/self/fd/1 /var/log/apache2/access.log && ln -sf /proc/self/fd/1 /var/log/apache2/error.log

EXPOSE 80

CMD /usr/sbin/apache2ctl -D FOREGROUND








#FROM python:3.9
#
#ADD . /ccvgd-backend
#WORKDIR /ccvgd-backend
#ENV PYTHONPATH=/ccvgd-backend
#
## Install any needed packages specified in requirements_1.txt
#COPY requirements.txt /ccvgd-backend
#RUN pip3 install -r requirements.txt
#
## Run app.py when the container launches
#COPY app.py /app
#CMD python3 app.py runserver -h 0.0.0.0 -p 80

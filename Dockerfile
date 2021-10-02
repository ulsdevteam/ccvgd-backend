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

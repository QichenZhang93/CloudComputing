FROM ubuntu
RUN apt-get update && apt-get install -y python && apt-get install -y lua5.1 && apt-get install -y perl && apt-get install -y ruby && \
	apt-get install -y python-pip python-dev build-essential && pip install --upgrade pip && pip install --upgrade virtualenv && \
	pip install Flask
ADD handle.py /home/
ADD index.html /home/
EXPOSE 5000
ENTRYPOINT python /home/handle.py

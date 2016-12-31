# Create the image for your Playground container by
# filling out this file.
# This directory is also a good place to put files
# which you would like to copy into the image.
FROM ubuntu
RUN apt-get update && apt-get install -y python && apt-get install -y ruby && \
	apt-get install -y perl && apt-get install -y lua5.1
#ADD ./hello.py /home/hello.py
#ENTRYPOINT python /home/hello.py
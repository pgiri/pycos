# File to build docker (https://www.docker.com) images to run
# dispycosnode containers.

# This file builds pycos (http://pycos.sourceforge.io) with Python 2
# using latest Ubuntu Linux.

FROM ubuntu:latest

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yq libpython2.7-dev python-pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  pip install pycos psutil netifaces

CMD ["/usr/local/bin/dispycosnode.py"]

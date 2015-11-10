FROM phusion/baseimage:0.9.17


RUN apt-get update
RUN apt-get install -y python-pip ipython python-dev libpq-dev git
RUN pip install --upgrade pip

RUN mkdir -p /etc/service/app
ADD run /etc/service/app/run
RUN chmod +x /etc/service/app/run

WORKDIR /app
RUN git clone https://github.com/adsabs/ADSOrcid /app
RUN git pull && git reset --hard HEAD


ADD gitpull.cronjob /
ADD gitpull.sh /
RUN chmod +x /gitpull.sh
RUN crontab /gitpull.cronjob


RUN mkdir -p /etc/service/app
ADD run /etc/service/app/run
RUN chmod +x /etc/service/app/run

CMD /bin/bash
[![Build Status](https://travis-ci.org/adsabs/ADSOrcid.svg)](https://travis-ci.org/adsabs/ADSOrcid)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSOrcid/badge.svg)](https://coveralls.io/r/adsabs/ADSOrcid)

# ADSOrcid

ORCID metadata enrichment pipeline - grabs claims from the API and enriches our storage/index.

dev setup - vagrant (docker)
============================

This is the easiest option. It will create a virtual machine using vagrant, start the required services (e.g., RabbitMQ) via docker. The  directory is synced to /vagrant/ on the guest.

1. `vagrant up`
1. `vagrant ssh app`
1. `cd /vagrant`


RabbitMQ
========

Access the GUI: http://localhost:8073

To start only the rabbitmq container:

`vagrant up rabbitmq`


Database
========

To start only the db container:

`vagrant up db`

The files are stored inside data/mongodb and data/postgres.




production setup
================

The vagrant (docker provider) can also be used to run production code. You probably only want to start the
application and connect to existing (external) databases and RabbitMQ. To do that:

1. create and edit the `local_config.py`
1. `vagrant start app`


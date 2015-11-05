[![Build Status](https://travis-ci.org/adsabs/ADSOrcid.svg)](https://travis-ci.org/adsabs/ADSOrcid)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSOrcid/badge.svg)](https://coveralls.io/r/adsabs/ADSOrcid)

# ADSOrcid

ORCID metadata enrichment pipeline - grabs claims from the API and enriches ADS storage/index.

How it works:

    1. periodically check ADS API (using a special OAuth token that gives access to ORCID updates)
    1. fetches the claims and puts them into the RabbitMQ queue
    1. a worker grabs the claim and enriches it with information about the author (querying both
       public ORCID API for the author's name and ADS API for variants of the author name)
    1. given the info above, it updates MongoDB (collection orcid_claims) - it marks the claim
       either as 'verified' (if it comes from a user with an account in BBB) or 'unverified'
       
       (it is the responsibility of the ADS Import pipeline to pick orcid claims and send them to
       SOLR for indexing)
       
       

dev setup - vagrant (docker)
============================

We are using 'docker' provider (ie. instead of virtualbox VM, you run the processes in docker).
On some systems, it is necessary to do: `export VAGRANT_DEFAULT_PROVIDER=docker` or always 
specify `--provider docker' when you run vagrant.
 
The  directory is synced to /vagrant/ on the guest.

1. `vagrant up`
1. `vagrant ssh app`
1. `cd /vagrant`


RabbitMQ
========

To start only the rabbitmq container:

`vagrant up rabbitmq`

The RabbitMQ will be on localhost:6672. The administrative interface on localhost:25672.


Database
========

To start only the db container:

`vagrant up db`

It will have a MongoDB instance and PostgreSQL database.

The files are synces into data/mongodb and data/postgres.


Application
===========

For development, you want to run it locally. Always make sure that you have the lates
database by running: `alembic upgrade head`



production setup
================

The vagrant (docker provider) can run the production code. You probably only want to start the
application and connect to existing (external) databases and RabbitMQ. To do that:

1. create and edit the `local_config.py`
1. `vagrant start app`

Essentially, you have to point the app at remote MongoDB, RabbitMQ and SQLALCHEMY.
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

1. vim ADSOrcid/local_config.py #edit, edit
1. `vagrant up db rabbitmq app`
1. `vagrant ssh app`
1. `cd /vagrant`

This will start the pipeline inside the `app` container - if you have configured endpoints and
access tokens correctly, it starts fetching data from orcid.

We are using 'docker' provider (ie. instead of virtualbox VM, you run the processes in docker).
On some systems, it is necessary to do: `export VAGRANT_DEFAULT_PROVIDER=docker` or always 
specify `--provider docker' when you run vagrant.
 
The  directory is synced to /vagrant/ on the guest.


dev setup - local editing
=========================

If you (also) hate when stuff is unnecessarily complicated, then you can also run/develop locally
(using whatever editor/IDE/debugger you like)

1. virtualenv python
1. source python/bin/activate
1. pip install -r requirements.txt
1. pip install -r dev-requirements.txt
1. vagrant `up db rabbitmq`

This will setup python `virtualenv` and the database + rabbitmq. You can run the pipeline and 
tests locally. 


RabbitMQ
========

`vagrant up rabbitmq`

The RabbitMQ will be on localhost:6672. The administrative interface on localhost:25672.


Database
========

`vagrant up db`

MongoDB is on localhost:37017, PostgreSQL on localhost:6432



production setup
================

`vagrant up prod`

It will automatically download/install the latest release from the github (no, not
your local changes - only from github).

If you /ADSOrcid/prod_config.py is available, it will copy and use it in place of
`local_config.py`

No ports are exposed, no SSH access is possible. New releases will deployed automatically.

Typical installation:

1. `vim ADSOrcid/prod_config.py` # edit, edit...
1. `vagrant up prod`


production setup - docker way
=============================

1. cd manifests/production/app
2. docker build --name ADSOrcid -t ADSOrcid .
3. cd ../../.. 
4. vim prod_config.py # edit, edit...
4. dockerun -d -v .:/vagrant/ --name ADSOrcid ADSOrcid /sbin/my_init


Here are some useful commands:

- restart service

	`docker exec ADSOrcid sv restart app`

- tail log from one of the workers

	`docker exec ADSOrcid tail -f /app/logs/ClaimsImporter.log`

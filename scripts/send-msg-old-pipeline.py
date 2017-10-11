#!/usr/bin/env python

import pika
import json
import sys
import os


# This is a quick-fix script that connects to the localhost:5684; I'm using it to submit
# one-off messages to the old pipeline. I'd create a tunnel to the rabbit:
#   work-tunnel 5684 adsqb:5682
# then run this script with one argument; where to read orcid-ids from
#   python send-msg-old-pipeline.py /tmp/missing-profiles.txt


def publish(channel, msg):
    if not isinstance(msg, basestring):
        msg = json.dumps(msg)
        
    x = channel.basic_publish(exchange='ads-orcid',
                      routing_key='ads.orcid.fresh-claims',
                      body=msg)


def run(inputfile):
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5684, virtual_host='ads-orcid'))
    channel = connection.channel()
    channel.queue_declare(queue='ads.orcid.fresh-claims', durable=True)
    
    print 'established connection to', connection, channel

    if not os.path.exists(inputfile):
        raise Exception('%s does not exist' % inputfile)
    
    i = 0
    with open(inputfile, 'r') as f:
        for l in f:
            l = l.strip()
            if l:
                print 'sending', l
                publish(channel, {'orcidid': l})
                i += 1
    
    print 'done submitting', i, 'orcidids'


if __name__ == '__main__':
    run(sys.argv[1])
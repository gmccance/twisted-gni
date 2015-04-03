#!/usr/bin/env python

# mccance
# Needs:
#  pip install stompest.asyc
#  pip install twisted
#  pip install stompest

import json
import logging
import signal
import traceback
import socket

from twisted.internet import reactor, defer, task

from stompest.async import Stomp
from stompest.async.listener import Listener, SubscriptionListener
from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.error import StompConnectionError

# Basic single instance consumer thread
class Consumer(object):

    def __init__(self, hostnport, topic = '/topic/xx', handler=None, user=None, password=None):
        self.config = StompConfig(uri = 'failover:(tcp://%s)?startupMaxReconnectAttempts=3' % hostnport, login=user, passcode = password)
        self.topic = topic
        self.host = hostnport
        self.handler = handler
        self.client = None

    @defer.inlineCallbacks
    def run(self, sequence):
        self.sequence = sequence

        # Connect here, yielding until it's done
        self.client = yield Stomp(self.config).connect()
        print "[%s] Connected to host: %s" % (sequence, self.host)
        headers = {
            StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL,
            'activemq.prefetchSize': '100',
        }

        # Subscribe to relevant topic, registering handler method and error method if handler fails
        self.subtoken = yield self.client.subscribe(self.topic, headers,
                                 listener=SubscriptionListener(self.handler, onMessageFailed=self.handle_messagehander_error))

        # We're done - pass to next callback in the chain to connect next broker        
        defer.returnValue(sequence+1)


    # Called if the onMessqge handler fails: print and swallow exception    
    def handle_messagehander_error(self, connection, failure, frame, errorDestination):
        print "Caught and ignoring exception in onMessage handler: %s " % failure.args
        traceback.print_exc()

    # Called by signal handler to unsubscribe and disconnect from brokers
    @defer.inlineCallbacks
    def disconnect(self):
       try:
          print "Disconnecting from host: %s" % self.host 
          if self.client:
             self.client.unsubscribe(self.subtoken)
             self.client.disconnect()
             yield self.client.disconnected    # be gentle 
       except StompConnectionError:
           pass  # hey, we tried our best

    # Called if we really can;t connect to one of the brokers
    #   Swallows error - the other conenction will continue
    def handle_connect_error(self, failure):
        print "[%i] Error on connection to host %s: %s" % (self.sequence, self.host, failure.getErrorMessage())
        return self.sequence + 1 



# Basic signal handler catching CTRL-C and SIGTERM: call clean disconnect on
#   all broker connection and stop reactor
def register_signals(connectionsToDisconnect):
    def handle_signal(num, frame):
        k={1:"SIGHUP", 2:"SIGINT", 15:"SIGTERM"}
        print "Received signal - " + k[num]
        if num == 2 or num == 15:
            print "Shutting down ...."
            for conn in connectionsToDisconnect:
               conn.disconnect()
            reactor.stop()
    return handle_signal    


# This handles trhe incoming messages and stores some random state about them
class MessageHandler(object):
 
   def __init__(self):
       # dict, keyed on hostgroup, holding a Set of "host-exception" strings
       #  basically counting number of open exceptions on each hostgroup
       self.hostexceptions = {}

   # Called for each message received
   def onMessage(self, client, frame):

        # some things live in the message headers 
        toplevel_hg = frame.headers['m_toplevel_hostgroup']
        id = frame.headers['message-id']

        # ..others live in the message body
        d = json.loads(frame.body)

        exc = d['metadata']['metric_name']
        h = d['metadata']['entity']
        state = d['metadata']['state']
        print "Consumed[%s]: Received %s on host %s (%s)" % (id, exc, h, state)

        # if 'open' or 'active', add it or keep it
        # if 'closed', remove it
        key = "%s-%s" % (h,exc)
        if state == 'open' or state == 'active':
            self.hostexceptions.setdefault(toplevel_hg, set()).add(key)
        elif state == 'close':
            self.hostexceptions.get(toplevel_hg, set()).discard(key)

   # Dumps out some stats from the random state when called
   def report_status(self):
        print "Tracking exceptions on %i hostgroups" % len(self.hostexceptions)
        for hg in self.hostexceptions:
            print " --> Hostgroup %s has %i exceptions" % (hg, len(self.hostexceptions[hg]))




if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)

    # Get all host aliases for partitioned GNI brokers
    #  (we need to connect and consume from all of them)
    aliases = socket.getaddrinfo('*****.cern.ch',61213, 0, 0, socket.IPPROTO_TCP)
    hosts = [ "%s:%s" % (a[4][0], a[4][1]) for a in aliases ]

    # creds for broker
    user = '***'
    password = '***'

    # single instance onMessage will handle all messages
    m = MessageHandler()
    
    # Deferred chain to conect to each broker in sequence
    # (only one async connect attempt can be pending at a time with stompest.async)
    d = defer.Deferred()
    clients = []
    for h in hosts:
       c = Consumer(h, handler=m.onMessage, user=user, password=password)
       d.addCallback(c.run)
       d.addErrback(c.handle_connect_error)
       clients.append(c)
    d.callback(1)

    # Dump out some stats every 10 seconds
    regular_report = task.LoopingCall(m.report_status)
    regular_report.start(10)


    # Shutdown handlers to cleanly disconnect from brokers
    sighandler = register_signals(clients)
    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGHUP, sighandler)
    signal.signal(signal.SIGTERM, sighandler)

    # start reactor
    reactor.run()


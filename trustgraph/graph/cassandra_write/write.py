
"""
Graph writer.  Input is graph edge.  Writes edges to Cassandra graph.
"""

import pulsar
import base64
import os
import argparse
import time

from ... trustgraph import TrustGraph
from ... schema import Triple
from ... log_level import LogLevel
from ... base import Consumer

default_input_queue = 'graph-load'
default_subscriber = 'graph-write-cassandra'
default_graph_host='localhost'

class Processor(Consumer):

    def __init__(
            self,
            pulsar_host=None,
            input_queue=default_input_queue,
            subscriber=default_subscriber,
            graph_host=default_graph_host,
            log_level=LogLevel.INFO,
    ):

        super(Processor, self).__init__(
            pulsar_host=pulsar_host,
            log_level=log_level,
            input_queue=input_queue,
            subscriber=subscriber,
            input_schema=Triple,
        )

        self.tg = TrustGraph([graph_host])

        self.count = 0

    def handle(self, msg):

        v = msg.value()

        self.tg.insert(
            v.s.value,
            v.p.value,
            v.o.value
        )

        self.count += 1

        if (self.count % 1000) == 0:
            print(self.count, "...", flush=True)

    @staticmethod
    def add_args(parser):

        Consumer.add_args(
            parser, default_input_queue, default_subscriber,
        )

        parser.add_argument(
            '-g', '--graph-host',
            default="localhost",
            help=f'Graph host (default: localhost)'
        )

def run():

    Processor.start("graph-write-cassandra", __doc__)


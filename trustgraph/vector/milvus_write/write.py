
"""
Accepts entity/vector pairs and writes them to a Milvus store.
"""

import pulsar
from pulsar.schema import JsonSchema
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import base64
import os
import argparse
import time

from ... schema import VectorsAssociation
from ... log_level import LogLevel
from ... triple_vectors import TripleVectors
from ... base import Consumer

default_input_queue = 'vectors-load'
default_subscriber = 'vector-write-milvus'
default_store_uri = 'http://localhost:19530'

class Processor(Consumer):

    def __init__(
            self,
            pulsar_host=None,
            input_queue=default_input_queue,
            subscriber=default_subscriber,
            store_uri=default_store_uri,
            log_level=LogLevel.INFO,
    ):

        super(Processor, self).__init__(
            pulsar_host=pulsar_host,
            log_level=log_level,
            input_queue=input_queue,
            subscriber=subscriber,
            input_schema=VectorsAssociation,
        )

        self.vecstore = TripleVectors(store_uri)

    def handle(self, msg):

        v = msg.value()

        if v.entity.value != "":
            for vec in v.vectors:
                self.vecstore.insert(vec, v.entity.value)
    @staticmethod
    def add_args(parser):

        Consumer.add_args(
            parser, default_input_queue, default_subscriber,
        )

        parser.add_argument(
            '-t', '--store-uri',
            default="http://milvus:19530",
            help=f'Milvus store URI (default: http://milvus:19530)'
        )

def run():

    Processor.start("vector-write-milvus", __doc__)

    

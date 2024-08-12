#!/usr/bin/env python3

import pulsar
import _pulsar
from pulsar.schema import JsonSchema
import hashlib
import uuid

from . schema import GraphEmbeddingsRequest, GraphEmbeddingsResponse
from . schema import graph_embeddings_request_queue
from . schema import graph_embeddings_response_queue

# Ugly
ERROR=_pulsar.LoggerLevel.Error
WARN=_pulsar.LoggerLevel.Warn
INFO=_pulsar.LoggerLevel.Info
DEBUG=_pulsar.LoggerLevel.Debug

class GraphEmbeddingsClient:

    def __init__(
            self, log_level=ERROR,
            subscriber=None,
            input_queue=None,
            output_queue=None,
            pulsar_host="pulsar://pulsar:6650",
    ):

        if input_queue == None:
            input_queue = graph_embeddings_request_queue

        if output_queue == None:
            output_queue = graph_embeddings_response_queue

        if subscriber == None:
            subscriber = str(uuid.uuid4())

        self.client = pulsar.Client(
            pulsar_host,
            logger=pulsar.ConsoleLogger(log_level),
        )

        self.producer = self.client.create_producer(
            topic=input_queue,
            schema=JsonSchema(GraphEmbeddingsRequest),
            chunking_enabled=True,
        )

        self.consumer = self.client.subscribe(
            output_queue, subscriber,
            schema=JsonSchema(GraphEmbeddingsResponse),
        )

    def request(self, vectors, limit=10, timeout=500):

        id = str(uuid.uuid4())

        r = GraphEmbeddingsRequest(
            vectors=vectors,
            limit=limit,
        )

        self.producer.send(r, properties={ "id": id })

        while True:

            msg = self.consumer.receive(timeout_millis=timeout * 1000)

            mid = msg.properties()["id"]

            if mid == id:
                resp = msg.value().entities
                self.consumer.acknowledge(msg)
                return resp

            # Ignore messages with wrong ID
            self.consumer.acknowledge(msg)

    def __del__(self):

        if hasattr(self, "consumer"):
            self.consumer.close()
            
        if hasattr(self, "producer"):
            self.producer.flush()
            self.producer.close()
            
        self.client.close()

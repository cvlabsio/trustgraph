
import os
import argparse
import pulsar
import _pulsar
import time
import threading
from prometheus_client import start_http_server, Info

from ... log_level import LogLevel

class BaseProducer:

    def __init__(self, processor, producer):
        self.processor = processor
        self.producer = producer

    def send(self, msg, properties={}):
        self.producer.send(msg, properties)

class BaseConsumer:

    def __init__(self, processor, consumer):
        self.processor = processor
        self.consumer = consumer
        self.running = True

    def receive(self):
        return self.consumer.receive()

    def loop(self, handle):

        while self.running:

            msg = self.receive()

            try:

                handle(msg)

                # Acknowledge successful processing of the message
                self.consumer.acknowledge(msg)

            except Exception as e:

                print("Exception:", e, flush=True)

                # Message failed to be processed
                self.consumer.negative_acknowledge(msg)

    def run(self, handle):
        self.start(handle)
        self.join()

    def start(self, handle):
        self.thread = threading.Thread(target=self.loop, args=(handle,))
        self.thread.start()

    def stop(self):
        self.running = False

    def join(self):
        self.thread.join()

class BaseProcessor:

    default_pulsar_host = os.getenv("PULSAR_HOST", 'pulsar://pulsar:6650')

    def create_producer(self, topic, schema):
    
        producer = self.client.create_producer(
            topic=topic, schema=schema,
        )

        return BaseProducer(self, producer)

    def create_consumer(self, topic, subscriber, schema):

        consumer = self.client.subscribe(
            topic, subscriber, schema=schema,
        )

        return BaseConsumer(self, consumer)

    def __init__(self, **params):

        self.client = None

        if not hasattr(__class__, "params_metric"):
            __class__.params_metric = Info(
                'params', 'Parameters configuration'
            )

        # FIXME: Maybe outputs information it should not
        __class__.params_metric.info({
            k: str(params[k])
            for k in params
        })

        pulsar_host = params.get("pulsar_host", self.default_pulsar_host)
        log_level = params.get("log_level", LogLevel.INFO)

        self.pulsar_host = pulsar_host

        self.client = pulsar.Client(
            pulsar_host,
            logger=pulsar.ConsoleLogger(log_level.to_pulsar())
        )

    def __del__(self):

        if self.client:
            self.client.close()

    @staticmethod
    def add_args(parser):

        parser.add_argument(
            '-p', '--pulsar-host',
            default=__class__.default_pulsar_host,
            help=f'Pulsar host (default: {__class__.default_pulsar_host})',
        )

        parser.add_argument(
            '-l', '--log-level',
            type=LogLevel,
            default=LogLevel.INFO,
            choices=list(LogLevel),
            help=f'Output queue (default: info)'
        )

        parser.add_argument(
            '--metrics',
            action=argparse.BooleanOptionalAction,
            default=True,
            help=f'Metrics enabled (default: true)',
        )

        parser.add_argument(
            '-P', '--metrics-port',
            type=int,
            default=8000,
            help=f'Pulsar host (default: 8000)',
        )

    def run(self):
        raise RuntimeError("Something should have implemented the run method")

    @classmethod
    def start(cls, prog, doc):

        parser = argparse.ArgumentParser(
            prog=prog,
            description=doc
        )

        cls.add_args(parser)

        args = parser.parse_args()
        args = vars(args)

        if args["metrics"]:
            start_http_server(args["metrics_port"])

        while True:

            try:

                p = cls(**args)
                p.run()

            except KeyboardInterrupt:
                print("Keyboard interrupt.")
                return

            except _pulsar.Interrupted:
                print("Pulsar Interrupted.")
                return

            except Exception as e:

                print(type(e))

                print("Exception:", e, flush=True)
                print("Will retry...", flush=True)

                time.sleep(10)

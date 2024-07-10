
"""
Simple LLM service, performs text prompt completion using an Ollama service.
Input is prompt, output is response.
"""

import pulsar
from pulsar.schema import JsonSchema
import tempfile
import base64
import os
import argparse
from langchain_community.llms import Ollama
import time

from ... schema import TextCompletionRequest, TextCompletionResponse
from ... log_level import LogLevel

class Processor:

    def __init__(
            self,
            pulsar_host,
            input_queue,
            output_queue,
            subscriber,
            log_level,
            model,
            ollama,
    ):

        self.client = pulsar.Client(
            pulsar_host,
            logger=pulsar.ConsoleLogger(log_level.to_pulsar())
        )

        self.consumer = self.client.subscribe(
            input_queue, subscriber,
            schema=JsonSchema(TextCompletionRequest),
        )

        self.producer = self.client.create_producer(
            topic=output_queue,
            schema=JsonSchema(TextCompletionResponse),
        )

        self.llm = Ollama(base_url=ollama, model=model)

    def run(self):

        while True:

            msg = self.consumer.receive()

            try:

                v = msg.value()

	        # Sender-produced ID

                id = msg.properties()["id"]

                print(f"Handling prompt {id}...", flush=True)

                prompt = v.prompt
                response = self.llm.invoke(prompt)

                print("Send response...", flush=True)
                r = TextCompletionResponse(response=response)
                self.producer.send(r, properties={"id": id})

                print("Done.", flush=True)

                # Acknowledge successful processing of the message
                self.consumer.acknowledge(msg)

            except Exception as e:

                print("Exception:", e, flush=True)

                # Message failed to be processed
                self.consumer.negative_acknowledge(msg)

    def __del__(self):
        print("Closing")
        self.client.close()

def run():

    parser = argparse.ArgumentParser(
        prog='llm-ollama-text',
        description=__doc__,
    )

    default_pulsar_host = os.getenv("PULSAR_HOST", 'pulsar://pulsar:6650')
    default_input_queue = 'llm-complete-text'
    default_output_queue = 'llm-complete-text-response'
    default_subscriber = 'llm-ollama-text'

    parser.add_argument(
        '-p', '--pulsar-host',
        default=default_pulsar_host,
        help=f'Pulsar host (default: {default_pulsar_host})',
    )

    parser.add_argument(
        '-i', '--input-queue',
        default=default_input_queue,
        help=f'Input queue (default: {default_input_queue})'
    )

    parser.add_argument(
        '-s', '--subscriber',
        default=default_subscriber,
        help=f'Queue subscriber name (default: {default_subscriber})'
    )

    parser.add_argument(
        '-o', '--output-queue',
        default=default_output_queue,
        help=f'Output queue (default: {default_output_queue})'
    )

    parser.add_argument(
        '-l', '--log-level',
        type=LogLevel,
        default=LogLevel.INFO,
        choices=list(LogLevel),
        help=f'Output queue (default: info)'
    )

    parser.add_argument(
        '-m', '--model',
        default="gemma2",
        help=f'LLM model (default: gemma2)'
    )

    parser.add_argument(
        '-r', '--ollama',
        default="http://localhost:11434",
        help=f'ollama (default: http://localhost:11434)'
    )

    args = parser.parse_args()

    
    while True:

        try:

            p = Processor(
                pulsar_host=args.pulsar_host,
                input_queue=args.input_queue,
                output_queue=args.output_queue,
                subscriber=args.subscriber,
                log_level=args.log_level,
                model=args.model,
                ollama=args.ollama,
            )

            p.run()

        except Exception as e:

            print("Exception:", e, flush=True)
            print("Will retry...", flush=True)

        time.sleep(10)


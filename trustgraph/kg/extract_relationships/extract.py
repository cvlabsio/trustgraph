
"""
Simple decoder, accepts vector+text chunks input, applies entity
relationship analysis to get entity relationship edges which are output as
graph edges.
"""

from pulsar.schema import JsonSchema

from ... schema import VectorsChunk, Triple, VectorsAssociation, Source, Value
from ... log_level import LogLevel
from ... llm_client import LlmClient
from ... prompts import to_relationships
from ... rdf import RDF_LABEL, TRUSTGRAPH_ENTITIES
from ... base import ConsumerProducer

RDF_LABEL_VALUE = Value(value=RDF_LABEL, is_uri=True)

default_input_queue = 'vectors-chunk-load'
default_output_queue = 'graph-load'
default_subscriber = 'kg-extract-relationships'
default_vector_queue='vectors-load'

class Processor(ConsumerProducer):

    def __init__(
            self,
            pulsar_host=None,
            input_queue=default_input_queue,
            vector_queue=default_vector_queue,
            output_queue=default_output_queue,
            subscriber=default_subscriber,
            log_level=LogLevel.INFO,
    ):

        super(Processor, self).__init__(
            pulsar_host=pulsar_host,
            log_level=log_level,
            input_queue=input_queue,
            output_queue=output_queue,
            subscriber=subscriber,
            input_schema=VectorsChunk,
            output_schema=Triple,
        )

        self.vec_prod = self.client.create_producer(
            topic=vector_queue,
            schema=JsonSchema(VectorsAssociation),
        )

        self.llm = LlmClient(pulsar_host=pulsar_host)

    def to_uri(self, text):

        part = text.replace(" ", "-").lower().encode("utf-8")
        quoted = urllib.parse.quote(part)
        uri = TRUSTGRAPH_ENTITIES + quoted

        return uri

    def get_relationships(self, chunk):

        prompt = to_relationships(chunk)
        resp = self.llm.request(prompt)

        rels = json.loads(resp)

        return rels

    def emit_edge(self, s, p, o):

        t = Triple(s=s, p=p, o=o)
        self.producer.send(t)

    def emit_vec(self, ent, vec):

        r = VectorsAssociation(entity=ent, vectors=vec)
        self.vec_prod.send(r)

    def handle(self, msg):

        v = msg.value()
        print(f"Indexing {v.source.id}...", flush=True)

        chunk = v.chunk.decode("utf-8")

        g = rdflib.Graph()

        try:

            rels = self.get_relationships(chunk)
            print(json.dumps(rels, indent=4), flush=True)

            for rel in rels:

                s = rel["subject"]
                p = rel["predicate"]
                o = rel["object"]

                s_uri = self.to_uri(s)
                s_value = Value(value=str(s_uri), is_uri=True)

                p_uri = self.to_uri(p)
                p_value = Value(value=str(p_uri), is_uri=True)

                if rel["object-entity"]: 
                    o_uri = self.to_uri(o)
                    o_value = Value(value=str(o_uri), is_uri=True)
                else:
                    o_value = Value(value=str(o), is_uri=False)

                self.emit_edge(
                    s_value,
                    p_value,
                    o_value
                )

                # Label for s
                self.emit_edge(
                    s_value,
                    RDF_LABEL_VALUE,
                    Value(value=str(s), is_uri=False)
                )

                # Label for p
                self.emit_edge(
                    p_value,
                    RDF_LABEL_VALUE,
                    Value(value=str(p), is_uri=False)
                )

                if rel["object-entity"]: 
                    # Label for o
                    self.emit_edge(
                        o_value,
                        RDF_LABEL_VALUE,
                        Value(value=str(o), is_uri=False)
                    )

                self.emit_vec(s_value, v.vectors)
                self.emit_vec(p_value, v.vectors)
                if rel["object-entity"]: 
                    self.emit_vec(o_value, v.vectors)

        except Exception as e:
            print("Exception: ", e, flush=True)

        print("Done.", flush=True)

    @staticmethod
    def add_args(parser):

        ConsumerProducer.add_args(
            parser, default_input_queue, default_subscriber,
            default_output_queue,
        )

        parser.add_argument(
            '-c', '--vector-queue',
            default=default_vector_queue,
            help=f'Vector output queue (default: {default_vector_queue})'
        )

def run():

    Processor.start("kg-extract-relationships", __doc__)


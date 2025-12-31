from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import video_pb2
import frame_pb2
# 1. Setup Schema Registry Client
sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

# 3. Setup Producer
producer_conf = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    pass

def produce_frame(topic, key, protobuf_obj):

    protobuf_serializer = ProtobufSerializer(frame_pb2.Frame, schema_registry_client)

    producer.produce(
        topic=topic,
        value=protobuf_serializer(
            protobuf_obj,
            SerializationContext(topic, MessageField.VALUE)
        ),
        on_delivery=delivery_report
    )
    producer.flush()

package com.earthtv.api;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.Properties;

//Source: https://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html

public class KProducer {
    private final static String TOPIC = "input-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Producer<String, VideoOuterClass.Video> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put("auto.register.schemas", "true");
        return new KafkaProducer<>(props);
    }

    static void runProducer(final VideoOuterClass.Video message) throws Exception {
        Producer<String, VideoOuterClass.Video> producer = createProducer();

        try {
            final ProducerRecord<String, VideoOuterClass.Video> record = new ProducerRecord<>(TOPIC, "0", message);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent record (key=%s) to topic %s partition %d at offset %d%n",
                            record.key(), metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });

        } finally {
            producer.flush();
            producer.close();
        }
    }
}

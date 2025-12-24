package com.earthtv.api;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

//Source: https://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html

public class KProducer {
    private final static String TOPIC = "my-example-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        Producer<String, String> producer = createProducer();

        try {
            for (long index = 1; index <= sendMessageCount; index++) {
                final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "request<lon, lat>", "(-120, 40)");
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent record (key=%s value=%s) to topic %s partition %d at offset %d%n",
                                record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}

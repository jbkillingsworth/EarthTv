import com.earthtv.protos.Frame;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Properties;

public class FrameRouter {

    static final String JSON_SOURCE_TOPIC = "proto-sink";
    static final String PROTOBUF_SINK_TOPIC = "frames";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = buildRouterStream(
                bootstrapServers,
                schemaRegistryUrl
        );
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static KafkaStreams buildRouterStream(final String bootstrapServers,
                                          final String schemaRegistryUrl) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "frame-router-stream-conversion");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "frame-router-stream-conversion-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, Frame.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaProtobufSerde.class.getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100 * 1000);

        final ObjectMapper objectMapper = new ObjectMapper();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Frame> routerStream = builder.stream(JSON_SOURCE_TOPIC);

        // Define the TopicNameExtractor
        TopicNameExtractor<String, Frame> orderTopicNameExtractor = (key, frame, recordContext) -> {
            int status = frame.getStatus();
            return "frame-" + String.valueOf(status);
        };

        routerStream.to(orderTopicNameExtractor);

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}
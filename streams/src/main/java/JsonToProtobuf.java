import com.earthtv.protos.Video;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class JsonToProtobuf {

    static final String JSON_SOURCE_TOPIC = "dbserver1.public.request-topic";
    static final String PROTOBUF_SINK_TOPIC = "proto-sink";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = buildJsonToProtoStream(
                bootstrapServers,
                schemaRegistryUrl
        );
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static KafkaStreams buildJsonToProtoStream(final String bootstrapServers,
                                              final String schemaRegistryUrl) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-to-avro-stream-conversion");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "json-to-avro-stream-conversion-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaProtobufSerde.class.getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100 * 1000);

        final ObjectMapper objectMapper = new ObjectMapper();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> jsonToProtoStream = builder.stream(JSON_SOURCE_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));
        jsonToProtoStream.mapValues(v -> {
            JsonObject element = JsonParser.parseString(v).getAsJsonObject();

            JsonObject record = element.get("payload").getAsJsonObject().get("after").getAsJsonObject();

            Video.Builder messageBuilder = Video.newBuilder();
            messageBuilder.setVideoId(record.get("video_id").getAsInt());
            messageBuilder.setUserId(record.get("user_id").getAsInt());
            messageBuilder.setStart(record.get("start").getAsInt());
            messageBuilder.setEnd(record.get("end").getAsInt());
            messageBuilder.setLon(record.get("lon").getAsDouble());
            messageBuilder.setLat(record.get("lat").getAsDouble());
            messageBuilder.setMinLon(record.get("min_lon").getAsDouble());
            messageBuilder.setMaxLon(record.get("max_lon").getAsDouble());
            messageBuilder.setMinLat(record.get("min_lat").getAsDouble());
            messageBuilder.setMaxLat(record.get("max_lat").getAsDouble());
            messageBuilder.setStatus(record.get("status").getAsInt());

            Video message = messageBuilder.build();
            return message;
        }).filter((k,v) -> v != null).to(PROTOBUF_SINK_TOPIC);

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}
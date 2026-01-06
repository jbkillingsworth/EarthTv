import com.earthtv.protos.Frame;
import com.earthtv.protos.Image;
import com.earthtv.protos.Video;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class JsonToProtobuf {

//    static final String JSON_SOURCE_TOPIC = "dbserver1.public.request-topic";
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

    static Video buildVideo(JsonObject record) {
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
    }

    static Frame buildFrame(JsonObject record) {
        Frame.Builder messageBuilder = Frame.newBuilder();
        messageBuilder.setFrameId(record.get("frame_id").getAsInt());
        messageBuilder.setVideoId(record.get("video_id").getAsInt());
        messageBuilder.setFrameItemId(record.get("frame_item_id").getAsString());

        messageBuilder.setFrameItemHref(record.get("frame_item_href").getAsString());
        messageBuilder.setBlueHref(record.get("blue_href").getAsString());
        messageBuilder.setGreenHref(record.get("green_href").getAsString());
        messageBuilder.setRedHref(record.get("red_href").getAsString());
        messageBuilder.setMinLon(record.get("min_lon").getAsDouble());
        messageBuilder.setMaxLon(record.get("max_lon").getAsDouble());
        messageBuilder.setMinLat(record.get("min_lat").getAsDouble());
        messageBuilder.setMaxLat(record.get("max_lat").getAsDouble());
        messageBuilder.setCollectionTimeUtc(record.get("collection_time_utc").getAsInt());

        messageBuilder.setImageData(ByteString.copyFromUtf8(record.get("image_data").getAsString()));
        messageBuilder.setImgWidth(record.get("img_width").getAsInt());
        messageBuilder.setImgHeight(record.get("img_height").getAsInt());

        messageBuilder.setStatus(record.get("status").getAsInt());
        Frame message = messageBuilder.build();
        return message;
    }

    static Image buildImage(JsonObject record) {
        Image.Builder messageBuilder = Image.newBuilder();
        messageBuilder.setImageId(record.get("image_id").getAsInt());
        messageBuilder.setFrameId(record.get("frame_id").getAsInt());
        messageBuilder.setFrameId(record.get("data").getAsByte());
        messageBuilder.setStatus(record.get("status").getAsInt());
        Image message = messageBuilder.build();
        return message;
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

        List<String> requiredTopics = Arrays.asList("dbserver1.public.video", "dbserver1.public.frame",
                "dbserver1.public.image");

        try (AdminClient adminClient = AdminClient.create(streamsConfiguration)) {
            Set<String> topics = adminClient.listTopics().names().get();


            List<NewTopic> newTopics = requiredTopics.stream()
                    .filter(t -> !topics.contains(t))
                    .map(t -> new NewTopic(t, 3, (short) 1)) // Define partitions and replication
                    .collect(Collectors.toList());

            adminClient.createTopics(newTopics).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        final ObjectMapper objectMapper = new ObjectMapper();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> jsonToProtoStream = builder.stream(requiredTopics,
                Consumed.with(Serdes.String(), Serdes.String()));

        TopicNameExtractor<String, Video> videoTopicNameExtractor = (key, video, recordContext) -> {
            int status = video.getStatus();
            return "video-" + String.valueOf(status);
        };

        TopicNameExtractor<String, Frame> frameTopicNameExtractor = (key, frame, recordContext) -> {
            int status = frame.getStatus();
            return "frame-" + String.valueOf(status);
        };

        TopicNameExtractor<String, Image> imageTopicNameExtractor = (key, image, recordContext) -> {
            int status = image.getStatus();
            return "image-" + String.valueOf(status);
        };

        jsonToProtoStream.map((k,v) -> {
            JsonObject element = JsonParser.parseString(v).getAsJsonObject();
            JsonElement type = element.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table");
            JsonObject record = element.get("payload").getAsJsonObject().get("after").getAsJsonObject();
            return new KeyValue<>(type.getAsJsonPrimitive().getAsString(), record);
        })
        .filter((k,v) -> v != null)
        .split()
        .branch((key, value) -> key.equals("video"),
            Branched.withConsumer(ks -> ks
                    .mapValues(v -> JsonToProtobuf.buildVideo(v))
                    .to(videoTopicNameExtractor)
            ))
        .branch((key, value) -> key.equals("frame"),
            Branched.withConsumer(ks -> ks
                    .mapValues(v -> JsonToProtobuf.buildFrame(v))
                    .to(frameTopicNameExtractor)
            ))
        .branch((key, value) -> key.equals("image"),
                Branched.withConsumer(ks -> ks
                        .mapValues(v -> JsonToProtobuf.buildImage(v))
                        .to(imageTopicNameExtractor)
                ))
        .defaultBranch(Branched.withConsumer(ks -> ks.to("error")));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}
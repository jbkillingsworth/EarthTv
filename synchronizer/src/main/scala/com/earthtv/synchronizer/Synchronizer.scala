package com.earthtv.synchronizer

import com.earthtv.api.VideoOuterClass
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes

import java.util.Properties
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.common.serialization.ByteArraySerializer
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde

class Synchronizer {

  var props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-scala-example")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  val myValueSerde = new KafkaProtobufSerde[VideoOuterClass.Video]
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, myValueSerde.getClass)//"io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer")
  props.put("schema.registry.url", "http://localhost:8081")

  val synchronizerTopology = new SynchronizerTopology()

  def start(): Unit = {
    val streams = new KafkaStreams(synchronizerTopology.getTopology, props)
//    val myValueSerde = new KafkaProtobufSerde[VideoOuterClass.Video]
    streams.start()

//    // Graceful shutdown
//    sys.ShutdownHookThread {
//      streams.close()
//    }
  }
}

object Synchronizer {
  def main(args: Array[String]): Unit = {
    val synchronizer = new Synchronizer
    synchronizer.start()
  }
}

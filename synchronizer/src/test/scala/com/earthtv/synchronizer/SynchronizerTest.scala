package com.earthtv.synchronizer

import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.Properties
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.common.serialization.Serdes
import java.util.Properties
import scala.collection.JavaConverters._

class SynchronizerTest extends AnyFlatSpec with Matchers {

  "MyTopology" should "transform input to uppercase" in {

    val builder = new StreamsBuilder()
    val topology = builder.build()

    val synchronizer = new Synchronizer

//    val streamsConfiguration = new Properties()
//    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app")
//    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
//    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
//    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)

    val testDriver = new TopologyTestDriver(synchronizer.synchronizerTopology.getTopology, synchronizer.props)

    try {
      // Create topics (specify appropriate serdes/deserializers)
      val inputTopic: TestInputTopic[String, String] = testDriver.createInputTopic(
        "input-topic",
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )
      val outputTopic: TestOutputTopic[String, String] = testDriver.createOutputTopic(
        "output-topic",
        Serdes.String().deserializer(),
        Serdes.String().deserializer()
      )

      // Pipe input and assert output
      inputTopic.pipeInput("key1", "value1")
      val outputRecord = outputTopic.readKeyValue()
      outputRecord shouldBe "key1_"

    } finally {
      testDriver.close()
    }
  }
}

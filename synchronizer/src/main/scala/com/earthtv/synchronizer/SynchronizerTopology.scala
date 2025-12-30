package com.earthtv.synchronizer

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.Stores

class SynchronizerTopology {

  private val topology = new Topology

  topology.addSource("Source", "dbserver1.public.request-topic")

  private val storeBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("CountsStore"),
    Serdes.String(),
    Serdes.Long()
  )

  topology.addProcessor("Process", () => new SynchronizerProcessor(), "Source")

  topology.addStateStore(storeBuilder, "Process")

//  topology.addSink("Sink", "output-topic", "Process")
  val myValueSerde = new KafkaProtobufSerde[com.earthtv.protos.Video]
  topology.addSink(
    "Sink",
    "output-topic2",
    Serdes.String().serializer(), // Output Key: String
    myValueSerde.serializer(), // Output Value: Double
    "Process"
  );

  def getTopology: Topology = {
    topology
  }
}

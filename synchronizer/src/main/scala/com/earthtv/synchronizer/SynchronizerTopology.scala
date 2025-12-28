package com.earthtv.synchronizer

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.Stores

class SynchronizerTopology {

  private val topology = new Topology

  topology.addSource("Source", "input-topic")

  private val storeBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("CountsStore"),
    Serdes.String(),
    Serdes.Long()
  )

  topology.addProcessor("Process", () => new SynchronizerProcessor(), "Source")

  topology.addStateStore(storeBuilder, "Process")

  topology.addSink("Sink", "output-topic", "Process")

  def getTopology: Topology = {
    topology
  }

}

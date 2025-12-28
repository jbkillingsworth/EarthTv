package com.earthtv.synchronizer

import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.state.KeyValueStore
import com.earthtv.api.VideoOuterClass

import java.nio.charset.StandardCharsets

class SynchronizerProcessor extends Processor[String, VideoOuterClass.Video, String, String] {

  private var context: ProcessorContext[String, String] = _
  private var kvStore: KeyValueStore[String, Long] = _

  override def init(context: ProcessorContext[String, String]): Unit = {
    // Retrieve the state store by name
    this.context = context
    this.kvStore = context.getStateStore("CountsStore").asInstanceOf[KeyValueStore[String, Long]]
  }

  override def process(record: Record[String, VideoOuterClass.Video]): Unit = {
    val key = record.key()
    val oldValue = Option(kvStore.get(key)).getOrElse(0L)
    val newValue = oldValue + 1L

    kvStore.put(key, newValue)

    val outputRecord = new Record(key, key+"_", record.timestamp())

    this.context.forward(outputRecord)
  }

  override def close(): Unit = {} // Cleanup resources if necessary
}
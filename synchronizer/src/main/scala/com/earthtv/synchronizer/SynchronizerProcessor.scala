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

    // Update the state store
    kvStore.put(key, newValue)

//    val video: VideoOuterClass.Video = record.value()//VideoOuterClass.Video.parser().parseFrom(record.value())

    // Forward the result downstream
    val outputRecord = new Record(key, key+"_", record.timestamp())
//    this.process(outputRecord)
    this.context.forward(outputRecord)

//    context.forward(outputRecord) // Optional: send to next processor/sink
  }

  override def close(): Unit = {} // Cleanup resources if necessary
}
package com.earthtv.synchronizer

import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.state.KeyValueStore
import com.google.gson.JsonParser

import java.nio.charset.StandardCharsets
import java.time.Instant

class SynchronizerProcessor extends Processor[String, String, String, com.earthtv.protos.Video] {

  private var context: ProcessorContext[String, com.earthtv.protos.Video] = _
  private var kvStore: KeyValueStore[String, Long] = _

  override def init(context: ProcessorContext[String, com.earthtv.protos.Video]): Unit = {
    // Retrieve the state store by name
    this.context = context
    this.kvStore = context.getStateStore("CountsStore").asInstanceOf[KeyValueStore[String, Long]]
  }

  override def process(record: Record[String, String]): Unit = {

    val key = record.key()

//    val test = com.earthtv.protos.Video.parseFrom(record.value().getBytes())
    val jsonObject = JsonParser.parseString(record.value()).getAsJsonObject
    val data: com.google.gson.JsonObject = jsonObject.get("payload").getAsJsonObject.get("after").getAsJsonObject

    val builder = com.earthtv.protos.Video.newBuilder()
    builder.setVideoId(data.get("video_id").getAsInt)
    builder.setUserId(data.get("user_id").getAsInt)
    builder.setStart(data.get("start").getAsInt)
    builder.setEnd(data.get("end").getAsInt)
    builder.setLon(data.get("lon").getAsDouble)
    builder.setLat(data.get("lat").getAsDouble)
    builder.setMinLon(data.get("min_lon").getAsDouble)
    builder.setMaxLon(data.get("max_lon").getAsDouble)
    builder.setMinLat(data.get("min_lat").getAsDouble)
    builder.setMaxLat(data.get("max_lat").getAsDouble)
    builder.setStatus(data.get("status").getAsInt)

    val message: com.earthtv.protos.Video = builder.build()

    val oldValue = Option(kvStore.get(key)).getOrElse(0L)
    val newValue = oldValue + 1L

    kvStore.put(key, newValue)

//    val record2 = new Record[String, com.earthtv.protos.Video ]()
//    val outputRecord: Record[String, com.earthtv.protos.Video] = record.withValue(message)

    //    Record

    val newRecord: Record[String, com.earthtv.protos.Video] = new Record(
      "new-key",           // The String key
      message,  // Your custom object
      context.currentSystemTimeMs() // A timestamp
    );

    this.context.
  }

  override def close(): Unit = {} // Cleanup resources if necessary
}
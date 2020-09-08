package org.apache.kafka.streams.scala

import kafka.utils.LogCaptureAppender
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde
import org.hamcrest.CoreMatchers.hasItems
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Test

class SerdesUnitTest {

  @Test
  def shouldLogMessageWhenTimeWindowedSerdeIsUsed(): Unit = {

    Serdes.timeWindowedSerde(new TimeWindowedSerde[String]())
    val appender = LogCaptureAppender.createAndRegister()
    assertThat(
      appender.getMessages,
      hasItems(
        "Implicit `timeWindowedSerde` produces incorrect end times on deserialization. Explicitly declare a new TimeWindowedDeserializer instead."
      )
    )
  }

}

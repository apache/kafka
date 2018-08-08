package org.apache.kafka.streams.scala

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier, KStream => JStream}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.kstream.KStream
import org.easymock.EasyMock._
import org.easymock.{Capture, EasyMock}
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

class KStreamTest extends JUnitSuite {

  @Test def deprecatedTransformShouldCreateATrueSupplier(): Unit = {
    val jstream = createMock(classOf[JStream[String, Long]])

    val capture: Capture[TransformerSupplier[String, Long, KeyValue[Int, Double]]] = newCapture()

    expect(jstream.transform(EasyMock.capture(capture))).andReturn(createMock(classOf[JStream[Int, Double]])).once()

    replay(jstream)

    val kstream = new KStream(jstream)
    //noinspection ScalaDeprecation
    kstream.transform(new TestTransformer)

    verify(jstream)

    val supplier: TransformerSupplier[String, Long, KeyValue[Int, Double]] = capture.getValue
    val transformer1 = supplier.get()
    val transformer2 = supplier.get()

    // we should get two different instances
    Assert.assertEquals(new KeyValue(1, 1.0), transformer1.transform("A", 1L))
    Assert.assertEquals(new KeyValue(1, 1.0), transformer2.transform("A", 1L))
  }

  // demonstrating the condition under which the deprecated method yields incorrect behavior
  @Test def deprecatedTransformDoesNotCreateATrueSupplierWhenPassedAnInstance(): Unit = {
    val jstream = createMock(classOf[JStream[String, Long]])

    val capture: Capture[TransformerSupplier[String, Long, KeyValue[Int, Double]]] = newCapture()

    expect(jstream.transform(EasyMock.capture(capture))).andReturn(createMock(classOf[JStream[Int, Double]])).once()

    replay(jstream)

    val kstream = new KStream(jstream)
    val transformer = new TestTransformer
    //noinspection ScalaDeprecation
    kstream.transform(transformer)

    verify(jstream)

    val supplier: TransformerSupplier[String, Long, KeyValue[Int, Double]] = capture.getValue
    val transformer1 = supplier.get()
    val transformer2 = supplier.get()

    // since we pre-initalized the transformer, both instances are actually the same
    Assert.assertEquals(new KeyValue(1, 1.0), transformer1.transform("A", 1L))
    Assert.assertEquals(new KeyValue(2, 2.0), transformer2.transform("A", 1L))
  }

  private class TestTransformer extends Transformer[String, Long, (Int, Double)] {
    var transforms: Int = 0

    override def init(context: ProcessorContext): Unit = ???

    override def transform(key: String, value: Long): (Int, Double) = {
      transforms = transforms + 1
      (transforms, transforms)
    }

    override def close(): Unit = ???
  }
}

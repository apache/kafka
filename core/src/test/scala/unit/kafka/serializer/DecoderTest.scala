package unit.kafka.serializer

import java.util.Properties

import kafka.serializer._
import kafka.utils.VerifiableProperties
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

class DecoderTest extends JUnitSuite {

  val TEST_STRING = "స్కాలా serializer పరీక్ష"
  val UTF8 = "UTF8"
  val ASCII = "ASCII"
  val STRING_BYTES = TEST_STRING.getBytes(UTF8)

  @Test
  def fromBytesOfDefaultDecoderShouldReturnTheInputByteArray(): Unit = {
    val defaultDecoder = new DefaultDecoder
    Assert.assertEquals(STRING_BYTES, defaultDecoder.fromBytes(STRING_BYTES))
    Assert.assertNotEquals(TEST_STRING.getBytes, defaultDecoder.fromBytes(STRING_BYTES))
    Assert.assertNotEquals(TEST_STRING.getBytes(UTF8), defaultDecoder.fromBytes(STRING_BYTES))
    Assert.assertEquals(null, defaultDecoder.fromBytes(null))
  }

  @Test
  def fromBytesOfStringDecoderShouldUseUTF8AsDefaultToDecode(): Unit = {
    Assert.assertEquals(TEST_STRING, new StringDecoder().fromBytes(STRING_BYTES))
  }

  @Test
  def fromBytesOfStringDecoderWithUTF8EncodingShouldSuccessfullyDecodeUTF8String(): Unit = {
    Assert.assertEquals(TEST_STRING, stringDecoder(UTF8).fromBytes(STRING_BYTES))
  }

  private def stringDecoder(encoding: String): StringDecoder = {
    val properties = new Properties
    properties.setProperty("serializer.encoding", encoding)
    new StringDecoder(new VerifiableProperties(properties))
  }

  @Test
  def fromBytesOfStringDecoderWithASCIIEncodingShouldFailToDecodeUTF8String(): Unit = {
    Assert.assertNotEquals(TEST_STRING, stringDecoder(ASCII).fromBytes(STRING_BYTES))
  }
}
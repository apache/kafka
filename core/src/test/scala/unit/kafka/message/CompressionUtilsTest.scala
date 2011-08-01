package kafka.message

import junit.framework.TestCase
import kafka.utils.TestUtils

class CompressionUtilTest extends TestCase {

  

  def testSimpleCompressDecompress() {

    val messages = List[Message](new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))

    val message = CompressionUtils.compress(messages)

    val decompressedMessages = CompressionUtils.decompress(message)

    TestUtils.checkLength(decompressedMessages.iterator,3)

    TestUtils.checkEquals(messages.iterator, decompressedMessages.iterator)
  }

  def testComplexCompressDecompress() {

    val messages = List[Message](new Message("hi there".getBytes), new Message("I am fine".getBytes), new Message("I am not so well today".getBytes))

    val message = CompressionUtils.compress(messages.slice(0, 2))

    val complexMessages = List[Message](message):::messages.slice(2,3)

    val complexMessage = CompressionUtils.compress(complexMessages)

    val decompressedMessages = CompressionUtils.decompress(complexMessage)

    TestUtils.checkLength(decompressedMessages.iterator,2)

    TestUtils.checkLength(decompressedMessages.iterator,3)

    TestUtils.checkEquals(messages.iterator, decompressedMessages.iterator)
  }
}

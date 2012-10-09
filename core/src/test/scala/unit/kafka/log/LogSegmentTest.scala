package kafka.log

import junit.framework.Assert._
import java.util.concurrent.atomic._
import org.junit.{Test, Before, After}
import org.scalatest.junit.JUnit3Suite
import kafka.utils.TestUtils
import kafka.message._
import kafka.utils.SystemTime
import scala.collection._

class LogSegmentTest extends JUnit3Suite {
  
  val segments = mutable.ArrayBuffer[LogSegment]()
  
  def createSegment(offset: Long): LogSegment = {
    val msFile = TestUtils.tempFile()
    val ms = new FileMessageSet(msFile)
    val idxFile = TestUtils.tempFile()
    idxFile.delete()
    val idx = new OffsetIndex(idxFile, offset, 100)
    val seg = new LogSegment(ms, idx, offset, 10, SystemTime)
    segments += seg
    seg
  }
  
  def messages(offset: Long, messages: String*): ByteBufferMessageSet = {
    new ByteBufferMessageSet(compressionCodec = NoCompressionCodec, 
                             offsetCounter = new AtomicLong(offset), 
                             messages = messages.map(s => new Message(s.getBytes)):_*)
  }
  
  @After
  def teardown() {
    for(seg <- segments) {
      seg.index.delete()
      seg.messageSet.delete()
    }
  }
  
  @Test
  def testReadOnEmptySegment() {
    val seg = createSegment(40)
    val read = seg.read(startOffset = 40, maxSize = 300, maxOffset = None)
    assertEquals(0, read.size)
  }
  
  @Test
  def testReadBeforeFirstOffset() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there", "little", "bee")
    seg.append(50, ms)
    val read = seg.read(startOffset = 41, maxSize = 300, maxOffset = None)
    assertEquals(ms.toList, read.toList)
  }
  
  @Test
  def testReadSingleMessage() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there")
    seg.append(50, ms)
    val read = seg.read(startOffset = 41, maxSize = 200, maxOffset = Some(50))
    assertEquals(new Message("hello".getBytes), read.head.message)
  }
  
  @Test
  def testReadAfterLast() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there")
    seg.append(50, ms)
    val read = seg.read(startOffset = 52, maxSize = 200, maxOffset = None)
    assertEquals(0, read.size)
  }
  
  @Test
  def testReadFromGap() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there")
    seg.append(50, ms)
    val ms2 = messages(60, "alpha", "beta")
    seg.append(60, ms2)
    val read = seg.read(startOffset = 55, maxSize = 200, maxOffset = None)
    assertEquals(ms2.toList, read.toList)
  }
  
  @Test
  def testTruncate() {
    val seg = createSegment(40)
    val ms = messages(50, "hello", "there", "you")
    seg.append(50, ms)
    seg.truncateTo(51)
    val read = seg.read(50, maxSize = 1000, None)
    assertEquals(1, read.size)
    assertEquals(ms.head, read.head)
  }
  
  @Test
  def testNextOffsetCalculation() {
    val seg = createSegment(40)
    assertEquals(40, seg.nextOffset)
    seg.append(50, messages(50, "hello", "there", "you"))
    assertEquals(53, seg.nextOffset())
  }
  
}
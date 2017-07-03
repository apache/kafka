package unit.kafka.tools

import java.io.{ByteArrayOutputStream, PrintStream}
import java.text.SimpleDateFormat

import kafka.tools.ConsumerPerformance
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}

/**
  * Created by lxy on 7/3/17.
  */
class ConsumerPerformanceTest {

  private val outContent: ByteArrayOutputStream = new ByteArrayOutputStream()

  @Before
  def setUp(): Unit = {
    System.setOut(new PrintStream(outContent))
  }

  @After
  def tearDown(): Unit = {
    System.setOut(null)
  }

  @Test
  def testPrintProgressMessage(): Unit = {
    ConsumerPerformance.printProgressMessage(1, 1024 * 1024, 0, 1, 0, 0, 1,
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    )
    val expected = "1970-01-01 08:00:00:001, 1, 1.0000, 1000.0000, 1, 1000.0000"
    assertEquals(expected, outContent.toString.trim)
  }

  @Test
  def testHeaderMatchBody(): Unit = {
    ConsumerPerformance.printHeader(true)
    val header = outContent.toString

    ConsumerPerformance.printProgressMessage(1, 1024 * 1024, 0, 1, 0, 0, 1,
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    )
    val body = outContent.toString.split("\n")(1)

    assertEquals(header.split(",").length, body.split(",").length)
  }


}

package kafka.utils

import junit.framework.Assert._
import org.junit.{Test, After, Before}

class JsonTest {

  @Test
  def testJsonEncoding() {
    assertEquals("null", Json.encode(null))
    assertEquals("1", Json.encode(1))
    assertEquals("1", Json.encode(1L))
    assertEquals("1", Json.encode(1.toByte))
    assertEquals("1", Json.encode(1.toShort))
    assertEquals("1.0", Json.encode(1.0))
    assertEquals("\"str\"", Json.encode("str"))
    assertEquals("true", Json.encode(true))
    assertEquals("false", Json.encode(false))
    assertEquals("[]", Json.encode(Seq()))
    assertEquals("[1,2,3]", Json.encode(Seq(1,2,3)))
    assertEquals("[1,\"2\",[3]]", Json.encode(Seq(1,"2",Seq(3))))
    assertEquals("{}", Json.encode(Map()))
    assertEquals("{\"a\":1,\"b\":2}", Json.encode(Map("a" -> 1, "b" -> 2)))
    assertEquals("{\"a\":[1,2],\"c\":[3,4]}", Json.encode(Map("a" -> Seq(1,2), "c" -> Seq(3,4))))
  }
  
}
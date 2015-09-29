package kafka.tools

import java.util

import org.junit.Assert
import org.junit.Test

class SimplePropertiesParserTest {

  @Test
  def shouldParseGoodProperties: Unit = {
    val result: util.Map[String, String] = SimplePropertiesParser.propsFromFreeform("key1=val1,key2=val2")
    assert(result.size() == 2)
    assert(result.get("key1") == "val1")
    assert(result.get("key2") == "val2")
  }

  @Test
  def shouldFailOnBadProperties: Unit = {
    try {
      SimplePropertiesParser.propsFromFreeform("key1=val1,key2")
      Assert.fail("Should have thrown an exception")
    } catch {
      case e: IllegalArgumentException => // expected exception
    }
  }
}

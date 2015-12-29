package kafka.utils

import java.util.Properties
import org.junit.Assert._
import org.junit.Test

class VerifiablePropertiesTest {

  @Test
  def testContainsKeyWithoutDefaults() {
    val props = new Properties
    props.setProperty("foo", "bar")
    props.setProperty("baz", "qux")
    val vProps = new VerifiableProperties(props)
    assertTrue(vProps.containsKey("foo"))
    assertTrue(vProps.containsKey("baz"))
    assertEquals(vProps.getString("foo"), "bar")
    assertEquals(vProps.getString("baz"), "qux")
  }

  @Test
  def testContainsKeyWithDefaults() {
    val baseProps = new Properties
    baseProps.setProperty("zookeeper.connect", "localhost:2181")
    baseProps.setProperty("zookeeper.connection.timeout.ms", "2000")
    val props1 = new Properties(baseProps)
    props1.setProperty("group.id", "test-1")
    val props2 = new Properties(baseProps)
    props2.setProperty("group.id", "test-2")
    val vProps1 = new VerifiableProperties(props1)
    val vProps2 = new VerifiableProperties(props2)
    assertTrue(vProps1.containsKey("zookeeper.connect"))
    assertTrue(vProps1.containsKey("group.id"))
    assertTrue(vProps2.containsKey("zookeeper.connect"))
    assertTrue(vProps2.containsKey("group.id"))
  }

  @Test
  def testInheritanceOfDefaults() {
   val baseProps = new Properties
    baseProps.setProperty("zookeeper.connect", "localhost:2181")
    baseProps.setProperty("zookeeper.connection.timeout.ms", "2000")
    val props = new Properties(baseProps)
    props.setProperty("group.id", "test-1")
    val vProps = new VerifiableProperties(props)
    assertEquals(vProps.getString("zookeeper.connect"), "localhost:2181")
    assertEquals(vProps.getString("group.id"), "test-1")
  }
}
